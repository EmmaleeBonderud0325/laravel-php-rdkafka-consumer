# Laravel PHP Rdkafka Consumer

Opiniated High Level consumer for [greensight/laravel-phprdkafka](https://github.com/greensight/laravel-php-rdkafka)

## Installation

Firstly, you have to install and configure [greensight/laravel-phprdkafka](https://github.com/greensight/laravel-php-rdkafka)

Then,
```bash
composer require greensight/laravel-phprdkafka-consumer
```

Publish the config file with:
```bash
php artisan vendor:publish --provider="Greensight\LaravelPhpRdKafkaConsumer\LaravelPhpRdKafkaConsumerServiceProvider" --tag="kafka-consumer-config"
```

Now go to `config/kafka-consumer.php` and add handlers there.

## Usage

The package provides `php artisan kafka:consume {topic} {consumer=default} {--exit-by-timeout}` command that executes the first handler that matches given topic and consumer name. Consumer name is taken from greensight/laravel-phprdkafka config file.

Handlers in config have the following configuration options:

```php
   [
      'topic' => 'stage.crm.fact.registrations.1', // optional, handler fits all topics by default
      'class' => \App\Domain\Communication\SendConfirmationEmailAction::class,
      'consumer' => 'default', // optional, handler fits all consumers by default
      'type' => 'action', // optional, possible types are: `action` (run execute() method on the given class) and `job` (dispatch the given Laravel job). Defaults to `action`
      'consume_timeout' => 5000, // optional, 5000ms by default
   ]

```

Action handler example:

```php
class SendConfirmationEmailAction
{
    public function execute(Message $message): void
    {
        // var_dump($message->payload);
    }
}
```

Job handler example:

```php

use Greensight\LaravelPhpRdKafkaConsumer\AbstractKafkaJob;

class ConsumeMessageJob extends AbstractKafkaJob
{
    public function handle(): void
    {
        // var_dump($this->message->payload);
    }
}

```

You do not have to extend `AbstractKafkaJob`, it's optional.

## Testing

```bash
composer test
```

## Changelog

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed recently.

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
