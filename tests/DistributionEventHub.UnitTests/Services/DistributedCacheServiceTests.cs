using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Repository;
using Meshmakers.Octo.Common.DistributionEventHub.Sagas;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Moq;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

public class DistributedCacheServiceTests
{
    private readonly Mock<IRepositoryClient> _repositoryClientMock;
    private readonly Mock<ITenantResolver> _tenantResolverMock;
    private readonly Mock<IRepository> _repositoryMock;
    private readonly DistributedCacheService _sut;

    public DistributedCacheServiceTests()
    {
        _repositoryClientMock = new Mock<IRepositoryClient>();
        _tenantResolverMock = new Mock<ITenantResolver>();
        _repositoryMock = new Mock<IRepository>();
        _sut = new DistributedCacheService(_repositoryClientMock.Object, _tenantResolverMock.Object);

        // Default setup
        _tenantResolverMock
            .Setup(r => r.GetRepositoryNameAsync(It.IsAny<string>()))
            .ReturnsAsync("test-repo");
        _repositoryClientMock
            .Setup(c => c.GetRepositoryAsync("test-repo"))
            .ReturnsAsync(_repositoryMock.Object);
    }

    [Fact]
    public async Task CreateStreamAsync_UploadsToCorrectRepository()
    {
        // Arrange
        var tenantId = "tenant-1";
        var stream = new MemoryStream([1, 2, 3]);
        var contentType = "application/pdf";
        var fileName = "test.pdf";
        var expiry = TimeSpan.FromHours(24);
        var expectedKey = "cache-key-123";

        _repositoryMock
            .Setup(r => r.UploadBinaryAsync(stream, contentType, fileName, It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedKey);

        // Act
        var result = await _sut.CreateStreamAsync(tenantId, stream, contentType, fileName, expiry);

        // Assert
        result.Should().Be(expectedKey);
        _tenantResolverMock.Verify(r => r.GetRepositoryNameAsync(tenantId), Times.Once);
        _repositoryMock.Verify(r => r.UploadBinaryAsync(stream, contentType, fileName, It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateStreamAsync_WithoutExpiry_PassesNullExpiry()
    {
        // Arrange
        var tenantId = "tenant-1";
        var stream = new MemoryStream([1, 2, 3]);
        var expectedKey = "cache-key-123";

        _repositoryMock
            .Setup(r => r.UploadBinaryAsync(stream, "text/plain", "test.txt", It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedKey);

        // Act
        var result = await _sut.CreateStreamAsync(tenantId, stream, "text/plain", "test.txt");

        // Assert
        result.Should().Be(expectedKey);
    }

    [Fact]
    public async Task CreateOrUpdateStreamAsync_CallsRepositoryWithCorrectParameters()
    {
        // Arrange
        var tenantId = "tenant-1";
        var stream = new MemoryStream([1, 2, 3]);
        var contentType = "application/json";
        var fileName = "data.json";
        var expectedKey = "cache-key-456";

        _repositoryMock
            .Setup(r => r.UploadWithReplaceByFileNameBinaryAsync(stream, contentType, fileName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedKey);

        // Act
        var result = await _sut.CreateOrUpdateStreamAsync(tenantId, stream, contentType, fileName);

        // Assert
        result.Should().Be(expectedKey);
        _repositoryMock.Verify(r => r.UploadWithReplaceByFileNameBinaryAsync(stream, contentType, fileName, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetCacheStreamByIdAsync_FileExists_ReturnsCacheStream()
    {
        // Arrange
        var tenantId = "tenant-1";
        var cacheKey = "cache-key-123";
        var expectedStream = new MemoryStream([1, 2, 3]);
        var downloadHandlerMock = new Mock<IDownloadStreamHandler>();
        downloadHandlerMock.Setup(d => d.ContentType).Returns("application/pdf");
        downloadHandlerMock.Setup(d => d.Stream).Returns(expectedStream);
        downloadHandlerMock.Setup(d => d.Filename).Returns("test.pdf");

        _repositoryMock
            .Setup(r => r.DownloadBinaryAsync(cacheKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(downloadHandlerMock.Object);

        // Act
        var result = await _sut.GetCacheStreamByIdAsync(tenantId, cacheKey);

        // Assert
        result.Should().NotBeNull();
        result!.ContentType.Should().Be("application/pdf");
        result.FileName.Should().Be("test.pdf");
        result.Stream.Should().BeSameAs(expectedStream);
    }

    [Fact]
    public async Task GetCacheStreamByIdAsync_FileNotFound_ReturnsNull()
    {
        // Arrange
        var tenantId = "tenant-1";
        var cacheKey = "non-existent-key";

        _repositoryMock
            .Setup(r => r.DownloadBinaryAsync(cacheKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync((IDownloadStreamHandler?)null);

        // Act
        var result = await _sut.GetCacheStreamByIdAsync(tenantId, cacheKey);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task GetCacheStreamByFileNameAsync_FileExists_ReturnsCacheStream()
    {
        // Arrange
        var tenantId = "tenant-1";
        var fileName = "test.pdf";
        var binaryId = "binary-id-123";
        var expectedStream = new MemoryStream([1, 2, 3]);

        var downloadInfoMock = new Mock<IDownloadInfo>();
        downloadInfoMock.Setup(d => d.BinaryId).Returns(binaryId);
        downloadInfoMock.Setup(d => d.ContentType).Returns("application/pdf");
        downloadInfoMock.Setup(d => d.Filename).Returns(fileName);

        var downloadHandlerMock = new Mock<IDownloadStreamHandler>();
        downloadHandlerMock.Setup(d => d.Stream).Returns(expectedStream);

        _repositoryMock
            .Setup(r => r.GetBinaryByFileNameAsync(fileName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(downloadInfoMock.Object);
        _repositoryMock
            .Setup(r => r.DownloadBinaryAsync(binaryId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(downloadHandlerMock.Object);

        // Act
        var result = await _sut.GetCacheStreamByFileNameAsync(tenantId, fileName);

        // Assert
        result.Should().NotBeNull();
        result!.ContentType.Should().Be("application/pdf");
        result.FileName.Should().Be(fileName);
        result.Stream.Should().BeSameAs(expectedStream);
    }

    [Fact]
    public async Task GetCacheStreamByFileNameAsync_FileNotFound_ReturnsNull()
    {
        // Arrange
        var tenantId = "tenant-1";
        var fileName = "non-existent.pdf";

        _repositoryMock
            .Setup(r => r.GetBinaryByFileNameAsync(fileName, It.IsAny<CancellationToken>()))
            .ReturnsAsync((IDownloadInfo?)null);

        // Act
        var result = await _sut.GetCacheStreamByFileNameAsync(tenantId, fileName);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task GetCacheStreamByFileNameAsync_DownloadFails_ReturnsNull()
    {
        // Arrange
        var tenantId = "tenant-1";
        var fileName = "test.pdf";
        var binaryId = "binary-id-123";

        var downloadInfoMock = new Mock<IDownloadInfo>();
        downloadInfoMock.Setup(d => d.BinaryId).Returns(binaryId);

        _repositoryMock
            .Setup(r => r.GetBinaryByFileNameAsync(fileName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(downloadInfoMock.Object);
        _repositoryMock
            .Setup(r => r.DownloadBinaryAsync(binaryId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((IDownloadStreamHandler?)null);

        // Act
        var result = await _sut.GetCacheStreamByFileNameAsync(tenantId, fileName);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task DeleteCacheStreamAsync_CallsRepositoryDelete()
    {
        // Arrange
        var tenantId = "tenant-1";
        var cacheKey = "cache-key-123";

        // Act
        await _sut.DeleteCacheStreamAsync(tenantId, cacheKey);

        // Assert
        _repositoryMock.Verify(r => r.DeleteBinaryAsync(cacheKey, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task DeleteAllCacheStreamsAsync_CallsRepositoryDeleteAll()
    {
        // Arrange
        var tenantId = "tenant-1";

        // Act
        await _sut.DeleteAllCacheStreamsAsync(tenantId);

        // Assert
        _repositoryMock.Verify(r => r.DeleteAllBinariesWithExpiryAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task DeleteAllExpiredCacheStreamsAsync_CallsRepositoryWithCurrentTime()
    {
        // Arrange
        var tenantId = "tenant-1";

        // Act
        await _sut.DeleteAllExpiredCacheStreamsAsync(tenantId);

        // Assert
        _repositoryMock.Verify(r => r.DeleteAllExpiredBinariesAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public void IsConnected_ReturnsRepositoryClientState_True()
    {
        // Arrange
        _repositoryClientMock.Setup(c => c.IsConnected).Returns(true);

        // Act & Assert
        _sut.IsConnected.Should().BeTrue();
    }

    [Fact]
    public void IsConnected_ReturnsRepositoryClientState_False()
    {
        // Arrange
        _repositoryClientMock.Setup(c => c.IsConnected).Returns(false);

        // Act & Assert
        _sut.IsConnected.Should().BeFalse();
    }

    [Fact]
    public async Task CreateStreamAsync_ResolvesCorrectTenant()
    {
        // Arrange
        var tenantId = "specific-tenant";
        var stream = new MemoryStream([1, 2, 3]);

        _tenantResolverMock
            .Setup(r => r.GetRepositoryNameAsync(tenantId))
            .ReturnsAsync("specific-tenant-repo");
        _repositoryClientMock
            .Setup(c => c.GetRepositoryAsync("specific-tenant-repo"))
            .ReturnsAsync(_repositoryMock.Object);
        _repositoryMock
            .Setup(r => r.UploadBinaryAsync(It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("key");

        // Act
        await _sut.CreateStreamAsync(tenantId, stream, "text/plain", "test.txt");

        // Assert
        _tenantResolverMock.Verify(r => r.GetRepositoryNameAsync(tenantId), Times.Once);
        _repositoryClientMock.Verify(c => c.GetRepositoryAsync("specific-tenant-repo"), Times.Once);
    }
}
