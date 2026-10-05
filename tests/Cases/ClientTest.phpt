<?php

declare(strict_types=1);

namespace Contributte\RabbitMQ\Tests\Cases;

use Bunny\AbstractClient;
use Bunny\Exception\ClientException;
use Contributte\RabbitMQ\Connection\Client;
use Tester\Assert;
use Tester\Environment;
use Tester\TestCase;

require __DIR__ . '/../bootstrap.php';

final class ClientTest extends TestCase
{

	/** More than the single 8192-byte stream chunk one fread() returns. */
	private const STREAM_CHUNK_BYTES = 8192;

	private const PAYLOAD_BYTES = 3 * self::STREAM_CHUNK_BYTES + 1;

	public function testFeedReadBufferDrainsEverythingAvailable(): void
	{
		[$client, $local, $remote] = $this->createClientOnSocketPair();

		$written = $this->writeMoreThanOneChunk($remote);

		$this->feedReadBuffer($client);

		Assert::same($written, $this->readBufferLength($client));
		Assert::true(stream_get_meta_data($local)['blocked'], 'The stream is switched back to blocking mode');
	}

	public function testFeedReadBufferKeepsANonBlockingStreamNonBlocking(): void
	{
		[$client, $local, $remote] = $this->createClientOnSocketPair();
		stream_set_blocking($local, false);
		fwrite($remote, 'x');

		$this->feedReadBuffer($client);

		Assert::false(stream_get_meta_data($local)['blocked']);
	}

	public function testFeedReadBufferReturnsTheDataAPeerSentBeforeClosing(): void
	{
		[$client, , $remote] = $this->createClientOnSocketPair();

		$written = $this->writeMoreThanOneChunk($remote);
		fclose($remote);

		$this->feedReadBuffer($client);
		Assert::same($written, $this->readBufferLength($client));

		Assert::exception(function () use ($client): void {
			$this->feedReadBuffer($client);
		}, ClientException::class, 'Broken pipe or closed connection.');
	}

	public function testFeedReadBufferCopesWithANegotiatedFrameMaxOfZero(): void
	{
		[$client, , $remote] = $this->createClientOnSocketPair();
		$frameMax = new \ReflectionProperty(AbstractClient::class, 'frameMax');
		$frameMax->setAccessible(true);
		$frameMax->setValue($client, 0);
		$written = $this->writeMoreThanOneChunk($remote);

		$this->feedReadBuffer($client);

		Assert::same($written, $this->readBufferLength($client));
		Assert::same(0, $frameMax->getValue($client), 'The negotiated frameMax is left as it was');
	}

	/**
	 * Writes without blocking, so the test cannot hang where unix-socket buffers are small
	 * (about 8 KiB each way on macOS). Where the socket cannot hold more than one stream chunk
	 * the drain cannot be observed, so the test is skipped rather than failed.
	 *
	 * @param resource $remote
	 */
	private function writeMoreThanOneChunk($remote): int
	{
		stream_set_blocking($remote, false);
		$payload = str_repeat('x', self::PAYLOAD_BYTES);
		$written = 0;

		while ($written < self::PAYLOAD_BYTES) {
			$bytes = fwrite($remote, substr($payload, $written));
			if ($bytes === false || $bytes === 0) {
				break;
			}

			$written += $bytes;
		}

		if ($written <= self::STREAM_CHUNK_BYTES) {
			Environment::skip('The socket buffer holds no more than one stream chunk.');
		}

		return $written;
	}

	/**
	 * @return array{Client, resource, resource}
	 */
	private function createClientOnSocketPair(): array
	{
		$pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
		Assert::type('array', $pair);
		[$local, $remote] = $pair;

		$client = new Client();
		$streamProperty = new \ReflectionProperty(AbstractClient::class, 'stream');
		$streamProperty->setAccessible(true);
		$streamProperty->setValue($client, $local);

		return [$client, $local, $remote];
	}

	private function feedReadBuffer(Client $client): void
	{
		$method = new \ReflectionMethod(Client::class, 'feedReadBuffer');
		$method->setAccessible(true);
		$method->invoke($client);
	}

	private function readBufferLength(Client $client): int
	{
		$property = new \ReflectionProperty(AbstractClient::class, 'readBuffer');
		$property->setAccessible(true);

		return $property->getValue($client)->getLength();
	}

}

(new ClientTest())->run();
