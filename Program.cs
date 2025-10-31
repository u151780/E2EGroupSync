using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Identity;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Identity.Client;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureAppConfiguration(cfg =>
    {
        cfg.AddEnvironmentVariables();
        cfg.AddJsonFile("local.settings.json", optional: true, reloadOnChange: true);
    })
    .ConfigureServices((ctx, services) =>
    {
        services.AddHttpClient("graph").ConfigureHttpClient(c =>
        {
            c.BaseAddress = new Uri("https://graph.microsoft.com/v1.0/");
            c.Timeout = TimeSpan.FromSeconds(100);
        });
        services.AddHttpClient("dv").ConfigureHttpClient(c =>
        {
            var url = ctx.Configuration["DATAVERSE_URL"] ?? throw new InvalidOperationException("DATAVERSE_URL missing");
            c.BaseAddress = new Uri($"{url}/api/data/v9.2/");
            c.Timeout = TimeSpan.FromSeconds(100);
        });

        services.AddSingleton<TokenProvider>();
        services.AddSingleton<GraphClient>();
        services.AddSingleton<DataverseClient>();
    })
    .Build();

await host.RunAsync();

// ----------------- Models -----------------
public record SyncRequest(
    Guid groupId,
    Guid? teamId,
    string? teamName,
    bool removeOrphans = true
);

public record SyncResult(
    Guid teamId,
    int adds,
    int removes,
    int skippedNotInEnvironment,
    string[] addedAadIds,
    string[] removedAadIds,
    string[] notInEnvironmentAadIds
);

// ---------------- Token Provider ----------------
public class TokenProvider
{
    private readonly IConfiguration _cfg;
    private readonly string _tenant;
    private readonly string? _clientId;
    private readonly string? _clientSecret;
    private readonly bool _useMI;

    public TokenProvider(IConfiguration cfg)
    {
        _cfg = cfg;
        _tenant = cfg["TENANT_ID"] ?? throw new InvalidOperationException("TENANT_ID missing");
        _clientId = cfg["CLIENT_ID"];
        _clientSecret = cfg["CLIENT_SECRET"];
        _useMI = string.Equals(cfg["USE_MANAGED_IDENTITY"], "true", StringComparison.OrdinalIgnoreCase);
    }

    public async Task<string> GetGraphTokenAsync()
    {
        var scope = _cfg["GRAPH_SCOPE"] ?? "https://graph.microsoft.com/.default";
        return await GetTokenAsync(scope);
    }

    public async Task<string> GetDataverseTokenAsync()
    {
        var scope = _cfg["DATAVERSE_SCOPE"]
            ?? throw new InvalidOperationException("DATAVERSE_SCOPE missing");
        return await GetTokenAsync(scope);
    }

    private async Task<string> GetTokenAsync(string scope)
    {
        if (_useMI)
        {
            var cred = new DefaultAzureCredential();
            var token = await cred.GetTokenAsync(new Azure.Core.TokenRequestContext(new[] { scope }));
            return token.Token;
        }
        else
        {
            if (string.IsNullOrEmpty(_clientId) || string.IsNullOrEmpty(_clientSecret))
                throw new InvalidOperationException("CLIENT_ID/CLIENT_SECRET missing");

            var app = ConfidentialClientApplicationBuilder
                .Create(_clientId)
                .WithClientSecret(_clientSecret)
                .WithTenantId(_tenant)
                .Build();

            var result = await app.AcquireTokenForClient(new[] { scope }).ExecuteAsync();
            return result.AccessToken;
        }
    }
}

// ---------------- Graph Client ----------------
public class GraphClient
{
    private readonly IHttpClientFactory _factory;
    private readonly TokenProvider _tokens;

    public GraphClient(IHttpClientFactory factory, TokenProvider tokens)
    {
        _factory = factory;
        _tokens = tokens;
    }

    public async IAsyncEnumerable<(string AadId, string UPN)> GetDirectGroupUserMembersAsync(Guid groupId)
    {
        var http = _factory.CreateClient("graph");
        http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", await _tokens.GetGraphTokenAsync());

        // Direct members only
        string? url = $"groups/{groupId}/members?$select=id,userPrincipalName&$top=999";
        while (!string.IsNullOrEmpty(url))
        {
            var resp = await http.GetAsync(url);
            resp.EnsureSuccessStatusCode();
            var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
            foreach (var m in json.RootElement.GetProperty("value").EnumerateArray())
            {
                if (m.TryGetProperty("@odata.type", out var t)
                    && t.GetString()?.EndsWith(".user", StringComparison.OrdinalIgnoreCase) == true)
                {
                    var id = m.GetProperty("id").GetString()!;
                    var upn = m.TryGetProperty("userPrincipalName", out var upnEl) ? upnEl.GetString() ?? "" : "";
                    yield return (id, upn);
                }
            }

            url = json.RootElement.TryGetProperty("@odata.nextLink", out var next)
                ? next.GetString()
                : null;

            // For nextLink, Graph returns absolute URL; strip base if needed
            if (!string.IsNullOrEmpty(url) && url.StartsWith("https://graph.microsoft.com/v1.0/"))
                url = url["https://graph.microsoft.com/v1.0/".Length..];
        }
    }
}

// ---------------- Dataverse Client ----------------
public class DataverseClient
{
    private readonly IHttpClientFactory _factory;
    private readonly TokenProvider _tokens;
    private readonly string _baseUrl;

    public DataverseClient(IHttpClientFactory factory, TokenProvider tokens, IConfiguration cfg)
    {
        _factory = factory;
        _tokens = tokens;
        _baseUrl = cfg["DATAVERSE_URL"] ?? throw new InvalidOperationException("DATAVERSE_URL missing");
    }

    private async Task<HttpClient> PrepareAsync(string named)
    {
        var http = _factory.CreateClient(named);
        http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", await _tokens.GetDataverseTokenAsync());
        http.DefaultRequestHeaders.Add("OData-MaxVersion", "4.0");
        http.DefaultRequestHeaders.Add("OData-Version", "4.0");
        return http;
    }

    public async Task<Guid> ResolveOwnerTeamAsync(Guid? teamId, string? teamName)
    {
        if (teamId.HasValue) return teamId.Value;

        var http = await PrepareAsync("dv");
        var filter = Uri.EscapeDataString($"name eq '{teamName?.Replace("'", "''")}' and teamtype eq 0");
        var resp = await http.GetAsync($"teams?$select=teamid,name&$filter={filter}&$top=1");
        resp.EnsureSuccessStatusCode();
        var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        var arr = json.RootElement.GetProperty("value");
        if (arr.GetArrayLength() == 0) throw new InvalidOperationException("Owner Team not found");
        return Guid.Parse(arr[0].GetProperty("teamid").GetString()!);
    }

    public async Task<Dictionary<string, (Guid SystemUserId, string AadId)>> GetCurrentTeamUsersAsync(Guid teamId)
    {
        var http = await PrepareAsync("dv");
        var fetch = $@"
<fetch version='1.0' mapping='logical' distinct='true'>
  <entity name='systemuser'>
    <attribute name='systemuserid' />
    <attribute name='azureactivedirectoryobjectid' />
    <filter>
      <condition attribute='isdisabled' operator='eq' value='0' />
    </filter>
    <link-entity name='teammembership' from='systemuserid' to='systemuserid' link-type='inner'>
      <link-entity name='team' from='teamid' to='teamid' link-type='inner'>
        <filter>
          <condition attribute='teamid' operator='eq' value='{teamId}' />
        </filter>
      </link-entity>
    </link-entity>
  </entity>
</fetch>";
        var url = $"systemusers?fetchXml={Uri.EscapeDataString(fetch)}";
        var result = new Dictionary<string, (Guid, string)>(StringComparer.OrdinalIgnoreCase);

        while (!string.IsNullOrEmpty(url))
        {
            var resp = await http.GetAsync(url);
            resp.EnsureSuccessStatusCode();
            var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
            foreach (var u in json.RootElement.GetProperty("value").EnumerateArray())
            {
                var su = Guid.Parse(u.GetProperty("systemuserid").GetString()!);
                var aad = u.TryGetProperty("azureactivedirectoryobjectid", out var aadEl) ? aadEl.GetString() ?? "" : "";
                if (!string.IsNullOrEmpty(aad))
                    result[aad] = (su, aad);
            }
            url = json.RootElement.TryGetProperty("@odata.nextLink", out var next)
                ? next.GetString()
                : null;
            if (!string.IsNullOrEmpty(url) && url.StartsWith($"{_baseUrl}/api/data/v9.2/"))
                url = url[$"{_baseUrl}/api/data/v9.2/".Length..];
        }

        return result;
    }

    public async Task<Guid?> GetSystemUserByAadIdAsync(string aadId)
    {
        var http = await PrepareAsync("dv");
        var filter = Uri.EscapeDataString($"azureactivedirectoryobjectid eq {aadId} and isdisabled eq false");
        var resp = await http.GetAsync($"systemusers?$select=systemuserid&$filter={filter}&$top=1");
        resp.EnsureSuccessStatusCode();
        var json = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        var arr = json.RootElement.GetProperty("value");
        if (arr.GetArrayLength() == 0) return null;
        return Guid.Parse(arr[0].GetProperty("systemuserid").GetString()!);
    }

    public async Task AssociateUserToTeamAsync(Guid teamId, Guid systemUserId)
    {
        var http = await PrepareAsync("dv");
        var body = new
        {
            @odataid = $"{_baseUrl}/api/data/v9.2/systemusers({systemUserId})"
        };
        var content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        var resp = await http.PostAsync($"teams({teamId})/teammembership_association/$ref", content);
        if (!resp.IsSuccessStatusCode && resp.StatusCode != HttpStatusCode.NoContent && resp.StatusCode != HttpStatusCode.Created)
        {
            var txt = await resp.Content.ReadAsStringAsync();
            throw new InvalidOperationException($"Associate failed: {resp.StatusCode} {txt}");
        }
    }

    public async Task DisassociateUserFromTeamAsync(Guid teamId, Guid systemUserId)
    {
        var http = await PrepareAsync("dv");
        var resp = await http.DeleteAsync($"teams({teamId})/teammembership_association(systemusers({systemUserId}))/$ref");
        if (!resp.IsSuccessStatusCode && resp.StatusCode != HttpStatusCode.NoContent)
        {
            var txt = await resp.Content.ReadAsStringAsync();
            throw new InvalidOperationException($"Disassociate failed: {resp.StatusCode} {txt}");
        }
    }
}

// ---------------- Function ----------------
public class SyncGroupToTeamFn
{
    private readonly GraphClient _graph;
    private readonly DataverseClient _dv;

    public SyncGroupToTeamFn(GraphClient graph, DataverseClient dv)
    {
        _graph = graph;
        _dv = dv;
    }

    [Function("SyncGroupToOwnerTeam")]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req)
    {
        var body = await new StreamReader(req.Body).ReadToEndAsync();
        var opts = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var input = JsonSerializer.Deserialize<SyncRequest>(body, opts)
                    ?? throw new InvalidOperationException("Invalid body");

        var teamId = await _dv.ResolveOwnerTeamAsync(input.teamId, input.teamName);

        // 1) Current Owner Team users (AAD ids)
        var current = await _dv.GetCurrentTeamUsersAsync(teamId);
        var currentAadIds = current.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);

        // 2) AAD group users (direct members)
        var aadIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await foreach (var (aad, _) in _graph.GetDirectGroupUserMembersAsync(input.groupId))
            aadIds.Add(aad);

        // 3) Diff
        var toAdd = aadIds.Except(currentAadIds).ToArray();
        var toRemove = input.removeOrphans ? currentAadIds.Except(aadIds).ToArray() : Array.Empty<string>();

        var added = new List<string>();
        var removed = new List<string>();
        var notInEnv = new List<string>();

        // 4) Add loop
        foreach (var aad in toAdd)
        {
            var su = await _dv.GetSystemUserByAadIdAsync(aad);
            if (su is null) { notInEnv.Add(aad); continue; }
            await _dv.AssociateUserToTeamAsync(teamId, su.Value);
            added.Add(aad);
        }

        // 5) Remove loop
        foreach (var aad in toRemove)
        {
            if (!current.TryGetValue(aad, out var tuple)) continue;
            await _dv.DisassociateUserFromTeamAsync(teamId, tuple.SystemUserId);
            removed.Add(aad);
        }

        var result = new SyncResult(
            teamId,
            added.Count,
            removed.Count,
            notInEnv.Count,
            added.ToArray(),
            removed.ToArray(),
            notInEnv.ToArray()
        );

        var res = req.CreateResponse(HttpStatusCode.OK);
        res.Headers.Add("Content-Type", "application/json");
        await res.WriteStringAsync(JsonSerializer.Serialize(result));
        return res;
    }
}
