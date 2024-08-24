<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Consumers\Factories;

use Ensi\LaravelPhpRdKafka\KafkaFacade;
use Ensi\LaravelPhpRdKafkaConsumer\ConsumerOptions;
use Ensi\LaravelPhpRdKafkaConsumer\Consumers\Consumer;
use Ensi\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerProcessorException;
use Ensi\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Ensi\LaravelPhpRdKafkaConsumer\ProcessorData;

class ConsumerFactory
{
    public function __construct(
        protected HighLevelConsumer $highLevelConsumer
    ) {
    }

    /**
     * @param string $topicKey
     * @param string $consumer
     * @return Consumer
     *
     * @throws KafkaConsumerProcessorException
     */
    public function build(string $topicKey, string $consumer = 'default'): Consumer
    {
        $processorData = $this->makeProcessorData($topicKey, $consumer);
        $consumerOptions = $this->makeConsumerOptions($consumer, $processorData);

        return new Consumer(
            highLevelConsumer: $this->highLevelConsumer,
            processorData: $processorData,
            consumerOptions: $consumerOptions,
            topicName: KafkaFacade::topicNameByClient('consumer', $consumer, $topicKey)
        );
    }

    /**
     * @param string $topicKey
     * @param string $consumer
     * @return ProcessorData
     *
     * @throws KafkaConsumerProcessorException
     */
    protected function makeProcessorData(string $topicKey, string $consumer): ProcessorData
    {
        $processorData = $this->findMatchedProcessor($topicKey, $consumer);

        if (!class_exists($processorData->class)) {
            throw new KafkaConsumerProcessorException("Processor class \"$processorData->class\" is not found");
        }

        if (!$processorData->hasValidType()) {
            throw new KafkaConsumerProcessorException("Invalid processor type \"$processorData->type\"," .
                " supported types are: " . implode(',', $processorData->getSupportedTypes()));
        }

        return $processorData;
    }

    /**
     * @param string $topicKey
     * @param string $consumer
     * @return ProcessorData
     *
     * @throws KafkaConsumerProcessorException
     */
    protected function findMatchedProcessor(string $topicKey, string $consumer): ProcessorData
    {
        foreach (config('kafka-consumer.processors', []) as $processor) {
            $topicMatched = empty($processor['topic']) || $processor['topic'] === $topicKey;
            $consumerMatched = empty($processor['consumer']) || $processor['consumer'] === $consumer;

            if ($topicMatched && $consumerMatched) {
                return new ProcessorData(
                    class: $processor['class'],
                    topicKey: $processor['topic'] ?? $topicKey,
                    consumer: $processor['consumer'] ?? $consumer,
                    type: $processor['type'] ?? 'action',
                    queue: $processor['queue'] ?? false,
                    consumeTimeout: $processor['consume_timeout'] ?? 20000,
                );
            }
        }

        throw new KafkaConsumerProcessorException("Processor for topic-key \"$topicKey\" and consumer \"$consumer\" is not found");
    }

    protected function makeConsumerOptions(string $consumer, ProcessorData $processorData): ConsumerOptions
    {
        $consumerPackageOptions = config('kafka-consumer.consumer_options.' . $consumer, []);

        return new ConsumerOptions(
            consumeTimeout: $consumerPackageOptions['consume_timeout'] ?? $processorData->consumeTimeout,
            middleware: $this->collectMiddleware($consumerPackageOptions['middleware'] ?? []),
        );
    }

    protected function collectMiddleware(array $processorMiddleware): array
    {
        return array_unique(
            array_merge(
                config('kafka-consumer.global_middleware', []),
                $processorMiddleware
            )
        );
    }
}
