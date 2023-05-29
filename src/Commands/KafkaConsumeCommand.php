<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Commands;

use Ensi\LaravelPhpRdKafka\KafkaFacade;
use Ensi\LaravelPhpRdKafkaConsumer\ConsumerOptions;
use Ensi\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Ensi\LaravelPhpRdKafkaConsumer\ProcessorData;
use Illuminate\Console\Command;
use Symfony\Component\Console\Command\SignalableCommandInterface;
use Throwable;

class KafkaConsumeCommand extends Command implements SignalableCommandInterface
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'kafka:consume
                            {topic-key : The key of a topic in the kafka.topics list}
                            {consumer=default : The name of the consumer}
                            {--max-events=0 : The number of events to consume before stopping}
                            {--max-time=0 : The maximum number of seconds the worker should run}
                            {--once : Only process the next event in the topic}
                            ';

    /**
     * The console command description.
     */
    protected $description = 'Consume concrete topic';

    protected ?HighLevelConsumer $consumer = null;

    public function getStopSignalsFromConfig(): array
    {
        return config('kafka-consumer.stop_signals', []);
    }

    public function getSubscribedSignals(): array
    {
        return $this->getStopSignalsFromConfig();
    }

    public function handleSignal(int $signal): void
    {
        if ($this->consumer && in_array($signal, $this->getStopSignalsFromConfig())) {
            $this->line("Stopping the consumer...");
            $this->consumer->forceStop();
        }
    }

    /**
     * Execute the console command.
     */
    public function handle(HighLevelConsumer $highLevelConsumer): int
    {
        $this->consumer = $highLevelConsumer;
        $topicKey = $this->argument('topic-key');
        $consumer = $this->argument('consumer');

        $processorData = $this->findMatchedProcessor($topicKey, $consumer);
        if (is_null($processorData)) {
            $this->error("Processor for topic-key \"$topicKey\" and consumer \"$consumer\" is not found");
            $this->line('Processors are set in /config/kafka-consumer.php');

            return 1;
        }

        if (!class_exists($processorData->class)) {
            $this->error("Processor class \"$processorData->class\" is not found");
            $this->line('Processors are set in /config/kafka-consumer.php');

            return 1;
        }

        if (!$processorData->hasValidType()) {
            $this->error("Invalid processor type \"$processorData->type\", supported types are: " . implode(',', $processorData->getSupportedTypes()));

            return 1;
        }

        $consumerPackageOptions = config('kafka-consumer.consumer_options.'. $consumer, []);
        $consumerOptions = new ConsumerOptions(
            consumeTimeout: $consumerPackageOptions['consume_timeout'] ?? $processorData->consumeTimeout,
            maxEvents: $this->option('once') ? 1 : (int) $this->option('max-events'),
            maxTime: (int) $this->option('max-time'),
            middleware: $this->collectMiddleware($consumerPackageOptions['middleware'] ?? []),
        );

        $topicName = KafkaFacade::topicNameByClient('consumer', $consumer, $topicKey);
        $this->info("Start listening to topic: \"{$topicKey}\" ({$topicName}), consumer \"{$consumer}\"");

        try {
            $highLevelConsumer
                ->for($consumer)
                ->listen($topicName, $processorData, $consumerOptions);
        } catch (Throwable $e) {
            $this->error('An error occurred while listening to the topic: '. $e->getMessage(). ' '. $e->getFile() . '::' . $e->getLine());

            return 1;
        }

        return 0;
    }

    protected function findMatchedProcessor(string $topic, string $consumer): ?ProcessorData
    {
        foreach (config('kafka-consumer.processors', []) as $processor) {
            $topicMatched = empty($processor['topic']) || $processor['topic'] === $topic;
            $consumerMatched = empty($processor['consumer']) || $processor['consumer'] === $consumer;
            if ($topicMatched && $consumerMatched) {
                return new ProcessorData(
                    class: $processor['class'],
                    topicKey: $processor['topic'] ?? null,
                    consumer: $processor['consumer'] ?? null,
                    type: $processor['type'] ?? 'action',
                    queue: $processor['queue'] ?? false,
                    consumeTimeout: $processor['consume_timeout'] ?? 20000,
                );
            }
        }

        return null;
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
