<?php

namespace Ensi\LaravelPhpRdKafkaConsumer\Commands;

use Ensi\LaravelPhpRdKafkaConsumer\Consumers\Consumer;
use Ensi\LaravelPhpRdKafkaConsumer\Consumers\Factories\ConsumerFactory;
use Ensi\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerException;
use Ensi\LaravelPhpRdKafkaConsumer\Exceptions\KafkaConsumerProcessorException;
use Ensi\LaravelPhpRdKafkaConsumer\Loggers\ConsumerLoggerFactory;
use Ensi\LaravelPhpRdKafkaConsumer\Loggers\ConsumerLoggerInterface;
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

    protected ?Consumer $consumer = null;

    public function __construct(protected ConsumerLoggerFactory $loggerFactory)
    {
        parent::__construct();
    }

    public function getStopSignalsFromConfig(): array
    {
        return config('kafka-consumer.stop_signals', []);
    }

    public function getSubscribedSignals(): array
    {
        return $this->getStopSignalsFromConfig();
    }

    public function getTopicKey(): string
    {
        return $this->argument('topic-key');
    }

    public function getConsumerName(): string
    {
        return $this->argument('consumer');
    }

    public function getMaxEvents(): int
    {
        return $this->option('once') ? 1 : (int) $this->option('max-events');
    }

    public function getMaxTime(): int
    {
        return (int) $this->option('max-time');
    }

    public function handleSignal(int $signal, int|false $previousExitCode = 0): int|false
    {
        if ($this->consumer) {
            $this->line("Stopping the consumer...");
            $this->consumer->forceStop();
        }

        return $previousExitCode;
    }

    /**
     * Execute the console command.
     */
    public function handle(ConsumerFactory $consumerFactory): int
    {
        try {
            $this->consumer = $consumerFactory
                ->build($this->getTopicKey(), $this->getConsumerName())
                ->setMaxEvents($this->getMaxEvents())
                ->setMaxTime($this->getMaxTime());

            $this->info("Start listening to topic: \"{$this->getTopicKey()}\"" .
                " ({$this->consumer->getTopicName()}), consumer \"{$this->getConsumerName()}\"");

            $this->consumer->listen();
        } catch (Throwable $exception) {
            $this->errorThrowable($exception);

            return self::FAILURE;
        }

        return self::SUCCESS;
    }

    private function errorThrowable(Throwable $exception): void
    {
        $this->makeLogger()
            ->error($exception->getMessage(), ['exception' => $exception]);

        if ($exception instanceof KafkaConsumerException) {
            $this->error($exception->getMessage());

            if ($exception instanceof KafkaConsumerProcessorException) {
                $this->line('Processors are set in /config/kafka-consumer.php');
            }

            return;
        }

        $this->error('An error occurred while listening to the topic: ' .
            $exception->getMessage() . ' ' . $exception->getFile() . '::' . $exception->getLine());
    }

    private function makeLogger(): ConsumerLoggerInterface
    {
        return $this->loggerFactory
            ->make($this->getTopicKey(), $this->getConsumerName());
    }
}
