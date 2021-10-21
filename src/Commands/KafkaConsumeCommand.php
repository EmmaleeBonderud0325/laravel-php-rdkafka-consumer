<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Commands;

use Ensi\LaravelPhpRdKafkaConsumer\ConsumerOptions;
use Ensi\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Throwable;
use Illuminate\Console\Command;

class KafkaConsumeCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'kafka:consume
                            {topic : The name of the topic}
                            {consumer=default : The name of the consumer}
                            {--max-events=0 : The number of events to consume before stopping}
                            {--max-time=0 : The maximum number of seconds the worker should run}
                            {--once : Only process the next event in the topic}
                            ';

    /**
     * The console command description.
     */
    protected $description = 'Consume concrete topic';

    /**
     * Execute the console command.
     */
    public function handle(): int
    {
        $topic = $this->argument('topic');
        $consumer = $this->argument('consumer');
        $availableConsumers = array_keys(config('kafka.consumers', []));

        if (!in_array($consumer, $availableConsumers)) {
            $this->error("Unknown consumer \"$consumer\"");
            $this->line('Available consumers are: "' . implode(', ', $availableConsumers) . '" and can be found in /config/kafka.php');

            return 1;
        }

        $processorData = $this->findMatchedProcessor($topic, $consumer);
        if (is_null($processorData)) {
            $this->error("Processor for topic \"$topic\" and consumer \"$consumer\" is not found");
            $this->line('Processors are set in /config/kafka-consumers.php');

            return 1;
        }

        $processorClassName = $processorData['class'];
        if (!class_exists($processorClassName)) {
            $this->error("Processor class \"$processorClassName\" is not found");
            $this->line('Processors are set in /config/kafka-consumers.php');

            return 1;
        }

        $supportedProcessorTypes = ['action', 'job'];
        $processorType = $processorData['type'] ?? 'action';
        if (!in_array($processorType, $supportedProcessorTypes)) {
            $this->error("Invalid processor type \"$processorType\", supported types are: " . implode(',', $supportedProcessorTypes));

            return 1;
        }

        $processorQueue = $processorData['queue'] ?? false;

        $consumerOptions = new ConsumerOptions(
            consumeTimeout: $processorData['consume_timeout'] ?? 20000,
            maxEvents: $this->option('once') ? 1 : (int) $this->option('max-events'),
            maxTime: (int) $this->option('max-time')
        );

        $this->info("Start listenning to topic: \"$topic\", consumer \"$consumer\"");
        try {
            $kafkaTopicListener = new HighLevelConsumer($topic, $consumer, $consumerOptions);
            $kafkaTopicListener->listen($processorClassName, $processorType, $processorQueue);
        } catch (Throwable $e) {
            $this->error('An error occurred while listening to the topic: '. $e->getMessage(). ' '. $e->getFile() . '::' . $e->getLine());

            return 1;
        }

        return 0;
    }

    protected function findMatchedProcessor(string $topic, string $consumer): ?array
    {
        foreach (config('kafka-consumer.processors', []) as $processor) {
            if (
                (empty($processor['topic']) || $processor['topic'] === $topic)
                && (empty($processor['consumer']) || $processor['consumer'] === $consumer)
                ) {
                return $processor;
            }
        }

        return null;
    }
}