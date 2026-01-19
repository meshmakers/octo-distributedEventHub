# Suggested Commands for Development

## Build Commands

### Standard Release Build
```bash
dotnet build src/**/*.csproj --configuration Release
```
Builds all projects in the src directory with Release configuration.

### Build with Private NuGet Server (Internal Development)
```bash
dotnet build src/**/*.csproj --configuration Release /p:OctoNugetPrivateServer=<server-url>
```
Use this when developing internally with access to the private NuGet server.

### Local Development Build (DebugL)
```bash
dotnet build --configuration DebugL
```
**Special configuration for local development:**
- Forces version to 999.0.0
- Adds local nuget restore path: `../nuget`
- Full debug symbols enabled
- Uses local NuGet packages for dependencies

### Build Entire Solution
```bash
dotnet build Octo.DistributedEventHub.sln --configuration Release
```

## Test Commands

### Run All Unit Tests (Excludes SystemTests)
```bash
dotnet test **/*Tests.csproj --configuration Release
```
**Note:** Currently, no test projects exist in the repository. This command is provided for future reference.

### Run Tests with Private NuGet Server
```bash
dotnet test --configuration Release /p:OctoNugetPrivateServer=<server-url>
```

## Running Sample Services

### Sample Service - Earth
```bash
dotnet run --project samples/SampleServiceEarth/SampleServiceEarth.csproj
```

### Sample Service - Mars
```bash
dotnet run --project samples/SampleServiceMars/SampleServiceMars.csproj
```

### Sample Service - Moon
```bash
dotnet run --project samples/SampleServiceMoon/SampleServiceMoon.csproj
```

### Sample CLI Tool
```bash
dotnet run --project samples/SampleCli/SampleCli.csproj
```
Command-line tool for testing message publishing and file caching.

**Prerequisites for running samples:**
- RabbitMQ running locally (default port 5672)
- MongoDB running locally (default port 27017)

## Clean and Restore

### Clean Build Artifacts
```bash
dotnet clean
```

### Restore NuGet Packages
```bash
dotnet restore
```

### Restore with Private NuGet Server
```bash
dotnet restore /p:OctoNugetPrivateServer=<server-url>
```

## Git Commands (macOS/Darwin)

### Check Repository Status
```bash
git status
```

### View Recent Commits
```bash
git log --oneline -10
```

### View Changes
```bash
git diff
```

### Commit Changes
```bash
git add .
git commit -m "Your commit message"
```

### Push to Remote
```bash
git push origin main
```

## Useful macOS System Commands

### List Files and Directories
```bash
ls -la
```

### Find Files by Name
```bash
find . -name "*.csproj"
```

### Search for Text in Files (grep)
```bash
grep -r "SearchTerm" src/
```

### View File Contents
```bash
cat path/to/file
```

### Change Directory
```bash
cd path/to/directory
```

### Print Working Directory
```bash
pwd
```

## NuGet Package Management

### Pack for Distribution
```bash
dotnet pack src/DistributionEventHub/DistributionEventHub.csproj --configuration Release
```

### Pack with Specific Version
```bash
dotnet pack --configuration Release /p:Version=1.2.3
```

## Development Workflow

### Typical Development Cycle
1. **Start infrastructure:**
   ```bash
   # Start RabbitMQ (if using Docker)
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management
   
   # Start MongoDB (if using Docker)
   docker run -d --name mongodb -p 27017:27017 mongo
   ```

2. **Build the project:**
   ```bash
   dotnet build --configuration DebugL
   ```

3. **Run sample services for testing:**
   ```bash
   # Terminal 1
   dotnet run --project samples/SampleServiceEarth/SampleServiceEarth.csproj
   
   # Terminal 2
   dotnet run --project samples/SampleServiceMars/SampleServiceMars.csproj
   
   # Terminal 3
   dotnet run --project samples/SampleCli/SampleCli.csproj
   ```

4. **Make code changes and rebuild:**
   ```bash
   dotnet build --configuration DebugL
   ```

5. **Commit changes:**
   ```bash
   git add .
   git commit -m "Description of changes"
   git push origin main
   ```

## Configuration Notes

### Build Configurations
- **Debug** - Standard debug configuration
- **Release** - Standard release configuration (for production)
- **DebugL** - Local development mode with version 999.0.0

### Version Management
Versions are automatically determined by:
- **DebugL configuration:** Always 999.0.0
- **With OctoNugetPrivateServer:** 0.1.*
- **Without OctoNugetPrivateServer:** 3.2.*

See `Directory.Build.props` for version configuration details.

## IDE Commands

### JetBrains Rider / Visual Studio
- **Build Solution:** Ctrl+Shift+B (Windows/Linux) or Cmd+Shift+B (macOS)
- **Run Project:** F5
- **Debug Project:** Shift+F5
- **Run Tests:** Ctrl+; (Windows/Linux) or Cmd+; (macOS)

## Troubleshooting Commands

### Clear NuGet Cache
```bash
dotnet nuget locals all --clear
```

### Verbose Build Output
```bash
dotnet build --verbosity detailed
```

### Check .NET Version
```bash
dotnet --version
```

### List Installed SDKs
```bash
dotnet --list-sdks
```
