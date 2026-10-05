<?php

declare(strict_types=1);

namespace Contributte\RabbitMQ\Connection;

use Bunny\Client as BunnyClient;
use Bunny\ClientStateEnum;
use Bunny\Exception\BunnyException;
use Bunny\Exception\ClientException;
use Bunny\Protocol\HeartbeatFrame;

class Client extends BunnyClient
{
	/**
	 * @throws BunnyException
	 */
	public function sendHeartbeat(): void {
		$this->getWriter()->appendFrame(new HeartbeatFrame(), $this->writeBuffer);
		$this->flushWriteBuffer();
	}

	/**
	 * Reads everything the stream can deliver right now, not just one chunk.
	 *
	 * Bunny's single fread() returns at most one stream chunk (8 KB). Over TLS the rest of a
	 * larger record is already decrypted and buffered inside PHP/OpenSSL, so the socket looks
	 * idle: run()'s stream_select() then sleeps until the next heartbeat (up to 60 s) although a
	 * complete delivery is waiting — a prefetch-1 consumer stalls on every message > 8 KB.
	 * Draining in non-blocking mode hands the whole delivery to the frame reader at once.
	 *
	 * @return bool
	 */
	protected function feedReadBuffer()
	{
		parent::feedReadBuffer();

		$stream = $this->getStream();
		$chunkLength = max(1, $this->frameMax);
		stream_set_blocking($stream, false);

		try {
			while (is_string($data = @fread($stream, $chunkLength)) && $data !== '') {
				$this->readBuffer->append($data);
				$this->lastRead = microtime(true);
			}
		} finally {
			stream_set_blocking($stream, true);
		}

		return true;
	}

	public function syncDisconnect(): bool
	{
		try {
			if ($this->state !== ClientStateEnum::CONNECTED) {
				return false;
			}

			$this->state = ClientStateEnum::DISCONNECTING;

			foreach ($this->channels as $channel) {
				$channelId = $channel->getChannelId();

				$this->channelClose($channelId, 0, '', 0, 0);
				$this->removeChannel($channelId);
			}

			$this->connectionClose(0, '', 0, 0);
			$this->closeStream();
		} catch (ClientException $e) {
			// swallow, we do not care we are not connected, we want to close connection anyway
		}

		$this->init();
		
		return true;
	}
}
