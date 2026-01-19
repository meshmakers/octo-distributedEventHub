# Code Style and Conventions

## C# Language Settings (Directory.Build.props)

### Language Version & Features
- **LangVersion:** `latestmajor` - Uses the latest major C# version features
- **Nullable:** `enable` - Nullable reference types are enabled throughout the project
- **ImplicitUsings:** `true` - Common namespaces are implicitly imported
- **TreatWarningsAsErrors:** `true` - All warnings are treated as compilation errors (zero-tolerance policy)

### Target Framework
- **Primary:** `net9.0` - .NET 9.0
- **Library:** `netstandard2.0` - For maximum compatibility in the main library

## Namespace Convention
All code uses the namespace pattern:
```csharp
namespace Meshmakers.Octo.Common.DistributionEventHub;
```

**Key conventions:**
- File-scoped namespaces (single line with semicolon)
- No nested namespace declarations
- Namespace matches the folder structure

## Documentation Standards

### XML Documentation Comments
All public APIs must have XML documentation comments:

```csharp
/// <summary>
///     Brief description of the class/method/property
/// </summary>
public class ExampleClass
{
    /// <summary>
    ///     Constructor description
    /// </summary>
    /// <param name="paramName">Parameter description</param>
    public ExampleClass(string paramName)
    {
    }
}
```

**Documentation style:**
- Use triple-slash (`///`) comments
- Include `<summary>` tags for all public members
- Include `<param>` tags for all parameters
- Include `<returns>` tags for methods that return values
- Use proper indentation (4 spaces for content inside tags)

## Access Modifiers

Strategic use of access modifiers:
- **public** - Public APIs exposed to consumers
- **internal** - Implementation details, helper classes, extension methods
- **private** - Instance-specific implementation

Example from codebase:
```csharp
internal static class TypeExtensions  // Internal utility class
{
    public static Type GetMostInnerBaseType(this Type type)  // Public method within internal class
    {
        // Implementation
    }
}
```

## Exception Handling Patterns

### Exception Class Structure
```csharp
public class DistributedOperationFailedException : DistributionException
{
    /// <summary>
    ///     Constructor
    /// </summary>
    public DistributedOperationFailedException()
    {
    }

    /// <summary>
    ///     Constructor
    /// </summary>
    /// <param name="message">Exception message</param>
    public DistributedOperationFailedException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Constructor
    /// </summary>
    /// <param name="message">Exception message</param>
    /// <param name="inner">Inner exception</param>
    public DistributedOperationFailedException(string message, Exception inner) : base(message, inner)
    {
    }

    // Internal static factory methods for common scenarios
    internal static DistributedOperationFailedException CreateCommandFailed(string commandName, Exception inner)
    {
        return new DistributedOperationFailedException(
            $"Command {commandName} failed.", inner);
    }
}
```

**Patterns:**
- Implement standard exception constructors (parameterless, message, message+inner)
- Use static factory methods for creating specific exception scenarios
- Factory methods are typically `internal` for implementation use

## Code Organization

### Class Structure Order
1. Fields (private)
2. Constructors
3. Public properties
4. Public methods
5. Internal methods
6. Private methods

### Naming Conventions
- **Classes/Interfaces:** PascalCase
- **Methods:** PascalCase
- **Properties:** PascalCase
- **Parameters:** camelCase
- **Local variables:** camelCase
- **Private fields:** Not observed in samples (rely on properties/auto-properties)
- **Constants:** PascalCase

### Interface Naming
Prefix with `I`:
- `IDistributionEventHubService`
- `ICommandClient<TRequest>`
- `IDistributedCacheService`

## Formatting

### Indentation
- **4 spaces** for indentation (no tabs)

### Braces
- Opening braces on the same line as the statement (K&R style):
```csharp
public class MyClass
{
    public void MyMethod()
    {
        if (condition)
        {
            // code
        }
    }
}
```

### Whitespace
- Blank line between methods
- No blank lines within method bodies unless separating logical blocks
- Single space after control flow keywords (`if`, `while`, `for`)

## Generic Type Parameters

Use descriptive generic type parameter names:
- `TRequest` - Request type
- `TResponse` - Response type
- `TMessage` - Message type
- `TConsumer` - Consumer type

Example:
```csharp
public interface ICommandClient<TRequest>
    where TRequest : class
{
    Task<Response<TResponse>> GetResponse<TResponse>(TRequest request)
        where TResponse : class;
}
```

## No External Style Configuration

**Note:** This project does NOT use:
- `.editorconfig` files
- StyleCop analyzers
- Custom analyzer packages

Style enforcement relies on:
- Visual Studio / Rider default C# formatting
- ReSharper settings (minimal, see `.sln.DotSettings`)
- Code review practices
- `TreatWarningsAsErrors=true` for compiler-enforced rules

## Best Practices Observed

1. **Prefer composition over inheritance** - Service interfaces are injected
2. **Use fluent configuration APIs** - `services.AddDistributionEventHub(config => { })`
3. **Dependency injection throughout** - All services registered in DI container
4. **Immutability where possible** - Use readonly fields, init-only properties
5. **Explicit null handling** - With nullable reference types enabled
6. **Async/await for I/O operations** - All external operations are async
7. **Strong typing** - Avoid dynamic types, use generics instead
