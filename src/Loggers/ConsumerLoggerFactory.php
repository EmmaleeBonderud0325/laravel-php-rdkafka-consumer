<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Loggers;

use Illuminate\Support\Facades\Log;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class ConsumerLoggerFactory
{
    public function make(string $topicKey, string $consumer): ConsumerLoggerInterface
    {
        return new ConsumerLogger(
            $this->makeLogger(),
            $topicKey,
            $consumer
        );
    }

    private function makeLogger(): LoggerInterface
    {
        $channel = config('kafka-consumer.log_channel', 'null');

        if (empty($channel) || $channel == 'null') {
            return new NullLogger();
        }

        return Log::channel($channel);
    }
}
