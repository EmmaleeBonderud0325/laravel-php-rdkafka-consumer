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
     * @param string $consumerName
     * @return Consumer
     *
     * @throws KafkaConsumerProcessorException
     */
    public function build(string $topicKey, string $consumerName = 'default'): Consumer
    {
        $processorData = $this->makeProcessorData($topicKey, $consumerName);
        $consumerOptions = $this->makeConsumerOptions($consumerName, $processorData);

        return new Consumer(
            highLevelConsumer: $this->highLevelConsumer,
            processorData: $processorData,
            consumerOptions: $consumerOptions,
            topicName: KafkaFacade::topicNameByClient('consumer', $consumerName, $topicKey)
        );
    }

    /**
     * @param string $topicKey
     * @param string $consumerName
     * @return ProcessorData
     *
     * @throws KafkaConsumerProcessorException
     */
    protected function makeProcessorData(string $topicKey, string $consumerName): ProcessorData
    {
        $processorData = $this->findMatchedProcessor($topicKey, $consumerName);

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
     * @param string $consumerName
     * @return ProcessorData
     *
     * @throws KafkaConsumerProcessorException
     */
    protected function findMatchedProcessor(string $topicKey, string $consumerName): ProcessorData
    {
        foreach (config('kafka-consumer.processors', []) as $processor) {
            $topicMatched = empty($processor['topic']) || $processor['topic'] === $topicKey;
            $consumerMatched = empty($processor['consumer']) || $processor['consumer'] === $consumerName;

            if ($topicMatched && $consumerMatched) {
                return new ProcessorData(
                    class: $processor['class'],
                    topicKey: $processor['topic'] ?? $topicKey,
                    consumer: $processor['consumer'] ?? $consumerName,
                    type: $processor['type'] ?? 'action',
                    queue: $processor['queue'] ?? false,
                    consumeTimeout: $processor['consume_timeout'] ?? 20000,
                );
            }
        }

        throw new KafkaConsumerProcessorException("Processor for topic-key \"$topicKey\" and consumer \"$consumerName\" is not found");
    }

    protected function makeConsumerOptions(string $consumerName, ProcessorData $processorData): ConsumerOptions
    {
        $consumerPackageOptions = config('kafka-consumer.consumer_options.' . $consumerName, []);

        return new ConsumerOptions(
            consumeTimeout: $consumerPackageOptions['consume_timeout'] ?? $processorData->consumeTimeout,
            middleware: $this->collectMiddleware($consumerPackageOptions['middleware'] ?? []),
        );
    }

    protected function collectMiddleware(array $processorMiddleware): array
    {
        return collect(config('kafka-consumer.global_middleware', []))
            ->merge($processorMiddleware)
            ->unique()
            ->values()
            ->toArray();
    }
}
