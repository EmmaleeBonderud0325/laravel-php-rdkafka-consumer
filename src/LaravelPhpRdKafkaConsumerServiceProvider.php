<?php

namespace Greensight\LaravelPhpRdKafkaConsumer;

use Greensight\LaravelPhpRdKafkaConsumer\Commands\KafkaConsumeCommand;
use Illuminate\Support\ServiceProvider;

class LaravelPhpRdKafkaConsumerServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        $this->mergeConfigFrom($this->packageBasePath("/../config/kafka-consumer.php"), 'kafka-consumer');
    }

    public function boot()
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                $this->packageBasePath("/../config/kafka-consumer.php") => config_path("kafka-consumer.php"),
            ], "kafka-consumer-config");

            $this->commands([
                KafkaConsumeCommand::class
            ]);
        }
    }

    protected function packageBasePath(string $directory = null): string
    {
        if ($directory === null) {
            return __DIR__;
        }

        return __DIR__ . DIRECTORY_SEPARATOR . ltrim($directory, DIRECTORY_SEPARATOR);
    }
}
