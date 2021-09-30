<?php

namespace Greensight\LaravelPhpRdKafkaConsumer\Commands;

use Greensight\LaravelPhpRdKafkaConsumer\HighLevelConsumer;
use Throwable;
use Illuminate\Console\Command;

class KafkaConsumeCommand extends Command
{
    /**
     * The name and signature of the console command.
     */
    protected $signature = 'kafka:consume {topic} {consumer=default} {--exit-by-timeout}';

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
        $exitByTimeout = (bool) $this->option('exit-by-timeout');
        $availableConsumers = array_keys(config('kafka.consumers', []));

        if (!in_array($consumer, $availableConsumers)) {
            $this->error("Unknown consumer \"$consumer\"");
            $this->line('Available consumers are: "' . implode(', ', $availableConsumers) . '" and can be found in /config/kafka.php');

            return 1;
        }

        $handlerData = $this->findMatchedHandler($topic, $consumer);
        if (is_null($handlerData)) {
            $this->error("Handler for topic \"$topic\" and consumer \"$consumer\" is not found");
            $this->line('Handlers are set in /config/kafka-consumers.php');

            return 1;
        }

        $handler = $handlerData['class'];
        if (!class_exists($handler)) {
            $this->error("Handler class \"$handler\" is not found");
            $this->line('Handlers are set in /config/kafka-consumers.php');

            return 1;
        }

        $consumeTimeout = $handlerData['consume_timeout'] ?? 5000;

        $allowedHandlerTypes = ['action', 'job'];
        $type = $handlerData['type'] ?? 'action';
        if (!in_array($type, $allowedHandlerTypes)) {
            $this->error("Invalid handler type \"$type\", allowed types are: " . implode(',', $allowedHandlerTypes));

            return 1;
        }

        $this->info("Start listenning to topic: \"$topic\", consumer \"$consumer\"");
        try {
            $kafkaTopicListener = new HighLevelConsumer($topic, $consumer, $consumeTimeout, $exitByTimeout);
            $kafkaTopicListener->listen($handler, $type);
        } catch (Throwable $e) {
            $this->error('An error occurred while listening to the topic: '. $e->getMessage(). ' '. $e->getFile() . '::' . $e->getLine());

            return 1;
        }

        return 0;
    }

    protected function findMatchedHandler(string $topic, string $consumer): ?array
    {
        foreach (config('kafka-consumer.handlers', []) as $handler) {
            if (
                (empty($handler['topic']) || $handler['topic'] === $topic)
                && (empty($handler['consumer']) || $handler['consumer'] === $consumer)
                ) {
                return $handler;
            }
        }

        return null;
    }
}