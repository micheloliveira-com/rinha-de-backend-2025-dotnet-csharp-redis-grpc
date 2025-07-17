var builder = WebApplication.CreateSlimBuilder(args);

var app = builder.Build();

var apiGroup = app.MapGroup("/");
apiGroup.MapGet("/", () => Results.Ok());

app.Run();
