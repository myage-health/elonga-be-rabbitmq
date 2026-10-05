<?php

declare(strict_types=1);

namespace Contributte\RabbitMQ\Tests\Cases;

use Bunny\AbstractClient;
use Bunny\Exception\ClientException;
use Contributte\RabbitMQ\Connection\Client;
use Tester\Assert;
use Tester\TestCase;

require __DIR__ . '/../bootstrap.php';

final class ClientTest extends TestCase
{

	private const PAYLOAD_BYTES = 3 * 8192 + 1;

	public function testFeedReadBufferDrainsEverythingAvailable(): void
	{
		[$client, $local, $remote] = $this->createClientOnSocketPair();

		// More than the single 8192-byte stream chunk one fread() returns.
		Assert::same(self::PAYLOAD_BYTES, fwrite($remote, str_repeat('x', self::PAYLOAD_BYTES)));

		$this->feedReadBuffer($client);

		Assert::same(self::PAYLOAD_BYTES, $this->readBufferLength($client));
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

		Assert::same(self::PAYLOAD_BYTES, fwrite($remote, str_repeat('x', self::PAYLOAD_BYTES)));
		fclose($remote);

		$this->feedReadBuffer($client);
		Assert::same(self::PAYLOAD_BYTES, $this->readBufferLength($client));

		Assert::exception(function () use ($client): void {
			$this->feedReadBuffer($client);
		}, ClientException::class, 'Broken pipe or closed connection.');
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
