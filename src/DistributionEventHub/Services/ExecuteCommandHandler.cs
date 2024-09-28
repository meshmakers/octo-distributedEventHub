namespace Meshmakers.Octo.Common.DistributionEventHub.Services;

/// <summary>
/// Delegate for executing commands
/// </summary>
/// <typeparam name="TMessage"></typeparam>
public delegate Task ExecuteCommandHandler<in TMessage>(TMessage message, RespondToCommandHandler respondToCommand);

/// <summary>
/// Delegate for handling responses to commands
/// </summary>
public delegate Task RespondToCommandHandler(object response);