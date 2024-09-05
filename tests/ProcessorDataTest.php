<?php

use Ensi\LaravelPhpRdKafkaConsumer\ProcessorData;

test('ProcessorData instantiable', function () {
    expect(new ProcessorData('foo', 'test', 'default'))->toBeObject();
});

test('ProcessorData has valid type by default', function () {
    $data = new ProcessorData('foo', 'test', 'default');
    expect($data->hasValidType())->toBeTrue();
});

test('ProcessorData can be constucted with invalid type', function () {
    $data = new ProcessorData(class: 'foo', topicKey: 'test', consumer: 'default', type: 'bar');
    expect($data->hasValidType())->toBeFalse();
});
