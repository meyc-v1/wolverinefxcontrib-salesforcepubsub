// Live Salesforce is shared state (topics, MES slots, replay rows) — never run tests in parallel.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

// One context for the whole run: user-secrets config, the REST publisher (publisher ECA), and the
// subscriber credentials handed to each test host.
[assembly: AssemblyFixture(typeof(WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness.SalesforceTestContext))]
