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

	public function testFeedReadBufferDrainsEverythingAvailable(): void
	{
		[$client, $local, $remote] = $this->createClientOnSocketPair();

		// Well above the 8 KB stream chunk a single fread() returns.
		$payload = str_repeat('x', 100000);
		fwrite($remote, $payload);

		$this->feedReadBuffer($client);

		Assert::same(strlen($payload), $this->readBufferLength($client));
		Assert::true(stream_get_meta_data($local)['blocked'], 'The stream is switched back to blocking mode');
	}

	public function testFeedReadBufferStillDetectsAClosedConnection(): void
	{
		[$client, , $remote] = $this->createClientOnSocketPair();

		fclose($remote);

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
