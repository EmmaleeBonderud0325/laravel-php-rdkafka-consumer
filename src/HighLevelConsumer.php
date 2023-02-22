<?php

namespace Ensi\LaravelPhpRdKafkaConsumer;

use Ensi\LaravelPhpRdKafka\KafkaManager;
use Ensi\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerException;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Pipeline\Pipeline;
use RdKafka\Exception as RdKafkaException;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\TopicPartition;
use Throwable;

class HighLevelConsumer
{
    protected ?KafkaConsumer $consumer;

    protected bool $forceStop = false;

    public function __construct(
        protected KafkaManager $kafkaManager,
        protected Pipeline $pipeline
    ) {
    }

    public function for(?string $consumerName): static
    {
        $this->consumer = is_null($consumerName)
            ? $this->kafkaManager->consumer()
            : $this->kafkaManager->consumer($consumerName);

        return $this;
    }

    public function forceStop(): static
    {
        $this->forceStop = true;

        return $this;
    }

    /**
     * @throws RdKafkaException
     * @throws Throwable
     */
    public function listen(string $topicName, ProcessorData $processorData, ConsumerOptions $options): void
    {
        $this->subscribe($topicName);

        [$startTime, $eventsProcessed] = [hrtime(true) / 1e9, 0];

        while (true) {
            $message = $this->consumer->consume($options->consumeTimeout);

            switch ($message->err) {

                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->processThroughMiddleware($processorData, $message, $options);
                    $this->consumer->commitAsync($message);
                    $eventsProcessed++;

                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // This also happens when there is no new messages in the topic after the specified timeout: https://github.com/arnaud-lb/php-rdkafka/issues/343
                    // We cannot differentiate broker timeout, poll timeout and eof timeout and are forced to keep on polling as a result.
                    // When kafka broker goes back online the connection will mostly likely be reestablished.
                    break;

                default:
                    throw new KafkaConsumerException('Kafka error: ' . $message->errstr());
            }

            if ($this->shouldBeStopped($startTime, $eventsProcessed, $options)) {
                break;
            }
        }
    }

    protected function processThroughMiddleware(ProcessorData $processorData, Message $message, ConsumerOptions $options): void
    {
        $this->pipeline
            ->send($message)
            ->through($options->middleware)
            ->then(fn (Message $message) => $this->executeProcessor($processorData, $message));
    }

    protected function executeProcessor(ProcessorData $processorData, Message $message): void
    {
        $processorData->queue
            ? $this->executeQueueableProcessor($processorData, $message)
            : $this->executeSyncProcessor($processorData, $message);
    }

    protected function executeSyncProcessor(ProcessorData $processorData, Message $message): void
    {
        /** @var class-string|Dispatchable $className */
        $className = $processorData->class;
        if ($processorData->type === 'job') {
            $className::dispatchSync($message);
        } elseif ($processorData->type === 'action') {
            resolve($className)->execute($message);
        }
    }

    protected function executeQueueableProcessor(ProcessorData $processorData, Message $message): void
    {
        /** @var class-string|Dispatchable $className */
        $className = $processorData->class;
        $queue = $processorData->queue;
        if ($processorData->type === 'job') {
            is_string($queue) ? $className::dispatch($message)->onQueue($queue) : $className::dispatch($message);
        } elseif ($processorData->type === 'action') {
            $processor = resolve($className);
            is_string($queue) ? $processor->onQueue($queue)->execute($message) : $processor->execute($message);
        }
    }

    protected function shouldBeStopped(int|float $startTime, int $eventsProcessed, ConsumerOptions $options): bool
    {
        if ($options->maxTime && hrtime(true) / 1e9 - $startTime >= $options->maxTime) {
            return true;
        }

        if ($options->maxEvents && $eventsProcessed >= $options->maxEvents) {
            return true;
        }

        return $this->forceStop;
    }

    protected function subscribe(string $topicName): void
    {
        $attempts = 0;
        do {
            $topicExists = (bool)$this->getPartitions($topicName);
            if (!$topicExists) {
                $this->consumer->newTopic($topicName);
                sleep(1);
            }
            $attempts++;
        } while (!$topicExists && $attempts < 10);

        $this->consumer->subscribe([$topicName]);
    }

    /**
     * @param string $topicName
     * @return TopicPartition[]
     * @throws RdKafkaException
     */
    public function getPartitions(string $topicName): array
    {
        $metadata = $this->consumer->getMetadata(true, null, 1000);
        foreach ($metadata->getTopics() as $topicMeta) {
            if ($topicMeta->getTopic() != $topicName) {
                continue;
            }
            $requestPartitions = [];
            foreach ($topicMeta->getPartitions() as $partitionMeta) {
                $requestPartitions[] = new TopicPartition($topicName, $partitionMeta->getId());
                $partitionMeta->getId();
            }

            return $this->consumer->getCommittedOffsets($requestPartitions, 1000);
        }

        return [];
    }

    public function getPartitionBounds(string $topicName, int $partitionId): array
    {
        $this->consumer->queryWatermarkOffsets($topicName, $partitionId, $low, $high, 1000);

        return [$low, $high];
    }

    /**
     * @throws RdKafkaException
     */
    public function setOffset(string $topicName, int $partitionId, int $offset): void
    {
        $this->consumer->commit([
            new TopicPartition(
                $topicName,
                $partitionId,
                $offset
            )
        ]);
    }
}
