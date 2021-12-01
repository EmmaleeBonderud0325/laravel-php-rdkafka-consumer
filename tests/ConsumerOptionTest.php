<?php

use Ensi\LaravelPhpRdKafkaConsumer\ConsumerOptions;

test('ConsumerOptions dto is instantiable', function () {
    expect(new ConsumerOptions())->toBeObject();
});
