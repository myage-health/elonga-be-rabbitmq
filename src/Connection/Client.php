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
	 * Upper bound of one drain. Well above what a prefetch-limited consumer can have in flight,
	 * yet it keeps a consumer without a prefetch limit on a backlogged queue from reading forever
	 * without ever processing a frame or sending a heartbeat.
	 */
	private const DRAIN_LIMIT_BYTES = 8 * 1024 * 1024;

	/** Read size when the broker negotiated frameMax 0 ("no limit"). */
	private const DEFAULT_READ_LENGTH = 65536;

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
	 * Bunny's single fread() returns at most one stream chunk (8192 bytes by default). Over TLS
	 * the rest of a larger record is already decrypted and buffered inside PHP/OpenSSL, so the
	 * socket looks idle: run()'s stream_select() then sleeps until the next heartbeat (up to 60 s)
	 * although a complete delivery is waiting — a prefetch-1 consumer stalls on every message
	 * larger than one chunk. Draining in non-blocking mode hands the whole delivery to the frame
	 * reader at once. EOF is left to the next regular read, which detects and reports it.
	 */
	protected function feedReadBuffer(): bool
	{
		$negotiatedFrameMax = $this->frameMax;
		$readLength = $negotiatedFrameMax > 0 ? $negotiatedFrameMax : self::DEFAULT_READ_LENGTH;

		// Bunny's read() passes frameMax to fread() as the length, and fread() rejects 0.
		$this->frameMax = $readLength;
		try {
			parent::feedReadBuffer();
		} finally {
			$this->frameMax = $negotiatedFrameMax;
		}

		$stream = $this->getStream();
		$wasBlocking = stream_get_meta_data($stream)['blocked'];
		stream_set_blocking($stream, false);

		try {
			$drained = 0;
			while ($drained < self::DRAIN_LIMIT_BYTES && is_string($data = @fread($stream, $readLength)) && $data !== '') {
				$this->readBuffer->append($data);
				$this->lastRead = microtime(true);
				$drained += strlen($data);
			}
		} finally {
			stream_set_blocking($stream, $wasBlocking);
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
