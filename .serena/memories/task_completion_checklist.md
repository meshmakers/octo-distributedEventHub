# Task Completion Checklist

When completing development tasks in the Octo.DistributedEventHub project, follow this checklist to ensure code quality and project standards are met.

## Pre-Commit Checklist

### 1. Build the Project
**Always build the project in Release configuration before committing:**

```bash
dotnet build src/**/*.csproj --configuration Release
```

**Why:** This project has `TreatWarningsAsErrors` set to `true`, which means all warnings are compilation errors. Building ensures no warnings or errors exist.

**For local development testing:**
```bash
dotnet build --configuration DebugL
```

### 2. Verify No Compiler Warnings/Errors
The project enforces zero-tolerance for warnings:
- Check build output for any warnings
- All warnings must be resolved before committing
- The build will fail if any warnings exist due to `TreatWarningsAsErrors=true`

### 3. Run Tests (If Applicable)
**Note:** Currently, no test projects exist in the repository.

When tests are added in the future:
```bash
dotnet test **/*Tests.csproj --configuration Release
```

### 4. Code Style Verification

#### Check for Required Documentation
- [ ] All public classes have XML documentation (`/// <summary>`)
- [ ] All public methods have XML documentation
- [ ] All parameters have `<param>` tags
- [ ] Return values have `<returns>` tags

#### Verify Coding Standards
- [ ] Nullable reference types handled properly (project has `Nullable=enable`)
- [ ] No use of `dynamic` types (prefer strong typing)
- [ ] Access modifiers are appropriate (public/internal/private)
- [ ] Naming conventions followed (PascalCase for classes/methods, camelCase for parameters)
- [ ] File-scoped namespaces used (`namespace X;` not `namespace X { }`)

### 5. Architecture Compliance

#### For New Messaging Features
- [ ] Instance prefix is properly applied to queue/exchange names
- [ ] Consumer definitions follow existing patterns (Broadcast vs Routed)
- [ ] Configuration follows fluent API pattern
- [ ] Dependency injection properly configured

#### For MongoDB/Caching Features
- [ ] Tenant isolation properly implemented via `ITenantResolver`
- [ ] Content-type properly handled
- [ ] TTL (Time-To-Live) support included where needed

### 6. Update CLAUDE.md (If Needed)
If you've made architectural changes, added new patterns, or changed configuration:

```bash
# Review and update if needed
cat CLAUDE.md
```

Update sections:
- [ ] Core messaging patterns (if new pattern added)
- [ ] Configuration pattern (if configuration API changed)
- [ ] Build commands (if build process changed)
- [ ] Key service classes (if new services added)

### 7. Version Considerations

Check which configuration you're working in:
- **DebugL** - Local development (version 999.0.0)
- **Release** - Production release
- **With OctoNugetPrivateServer** - Internal development

Ensure version implications are understood:
```bash
# Check current version in Directory.Build.props
cat Directory.Build.props | grep -A 3 "OctoVersion"
```

### 8. Dependency Check
If you've added new NuGet packages:
- [ ] Package is compatible with .NET 9.0 and netstandard2.0
- [ ] Version conflicts resolved
- [ ] Package properly referenced in .csproj file

### 9. Git Hygiene

#### Before Committing
```bash
# Check what's being committed
git status
git diff

# Verify no unintended files
```

#### Ensure Clean Commit
- [ ] No debug/temp files committed
- [ ] No sensitive information (API keys, passwords)
- [ ] No commented-out code blocks (remove or document why kept)
- [ ] Commit message is descriptive

### 10. Sample Application Updates (If Applicable)
If you've changed APIs or added new features:
- [ ] Update sample applications if they demonstrate the feature
- [ ] Ensure sample services still run correctly
- [ ] Test with RabbitMQ and MongoDB running

```bash
# Test sample services
dotnet run --project samples/SampleServiceEarth/SampleServiceEarth.csproj
```

## Post-Commit Verification

### 1. Verify Build on Clean Clone (Optional but Recommended)
For significant changes, verify the build works on a fresh clone:

```bash
cd /tmp
git clone <repository-url> test-clone
cd test-clone
dotnet build --configuration Release
```

### 2. Integration Testing (Manual)
If RabbitMQ and MongoDB are available:
- [ ] Run all sample services
- [ ] Verify messages are routed correctly
- [ ] Verify file caching works as expected
- [ ] Check RabbitMQ management UI for correct queue names with prefixes

## Special Considerations

### When Adding New Consumer Types
- [ ] Create appropriate consumer definition class
- [ ] Add configuration method to fluent API
- [ ] Update CLAUDE.md with usage example
- [ ] Consider creating sample in samples/ directory

### When Modifying Exception Types
- [ ] Implement all standard exception constructors
- [ ] Add XML documentation
- [ ] Consider adding static factory methods for common scenarios
- [ ] Update exception handling documentation if needed

### When Changing Configuration API
- [ ] Maintain backward compatibility if possible
- [ ] Update CLAUDE.md configuration examples
- [ ] Update sample applications
- [ ] Consider deprecation warnings for old API

### When Working with MongoDB/Caching
- [ ] Test with actual MongoDB instance
- [ ] Verify TTL cleanup works
- [ ] Test tenant isolation
- [ ] Verify GridFS file operations

## Checklist Summary

Quick pre-commit checklist:
1. ✅ Build succeeds: `dotnet build --configuration Release`
2. ✅ No warnings or errors
3. ✅ XML documentation complete for public APIs
4. ✅ Code style adheres to conventions
5. ✅ CLAUDE.md updated if needed
6. ✅ Git status clean (no unintended files)
7. ✅ Commit message is descriptive

**Remember:** The build will fail if ANY warnings exist due to `TreatWarningsAsErrors=true`. This is by design to maintain code quality.
