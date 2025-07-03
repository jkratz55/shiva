# Shiva

<p align="center">
  <img src=".github/images/final-fantasy-shiva.png" alt="Final Fantasy - Shiva"/>
</p>

[![Go Reference](https://pkg.go.dev/badge/github.com/jkratz55/shiva.svg)](https://pkg.go.dev/github.com/jkratz55/shiva)
[![Go Report Card](https://goreportcard.com/badge/github.com/jkratz55/shiva)](https://goreportcard.com/report/github.com/jkratz55/shiva)
[![License](https://img.shields.io/github/license/jkratz55/shiva)](https://github.com/jkratz55/shiva/blob/master/LICENSE)
[![Release](https://img.shields.io/github/v/release/jkratz55/shiva)](https://github.com/jkratz55/shiva/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/jkratz55/shiva)](https://go.dev/dl/)
[![Build Status](https://github.com/jkratz55/shiva/workflows/CI/badge.svg)](https://github.com/jkratz55/shiva/actions)
[![Coverage Status](https://coveralls.io/repos/github/jkratz55/shiva/badge.svg?branch=master)](https://coveralls.io/github/jkratz55/shiva?branch=master)

Shiva is a GO library/module for working with Kafka. Shiva provides friendly higher level APIs for consuming and
producing messages with Kafka. Under the hood Shiva uses the official Confluent Kafka GO
client (https://github.com/confluentinc/confluent-kafka-go). Some GO developers are very much opposed to using CGO, and
unfortunately, if you are dead set on avoiding CGO, this library may not be for you as it uses confluent-kafka-go, which
is a wrapper around librdkafka.

Shiva has a number of features that aim to make working with Kafka in GO easy:

* High-level and flexible Consumer API for consuming messages from Kafka
* High-level API for producing messages synchronously and asynchronously to Kafka.
* Support for OpenTelemetry tracing and metrics
* Built-in support for dead letter processing when a message cannot be processed
* Separates the concerns of Kafka from processing messages via the Handler interface

## Where Does the Name Shiva Come From?

Shiva is a frequently recurring Ice-elemental summon in the Final Fantasy series. Although enjoying regular appearances
throughout the series, Shiva, like most of the popular summonable entities, has not been given a significant back story,
being simply described as the "Ice Queen". As naming things can be quite hard, I've started naming my libraries and
packages based on video game lore and universes.

## Quickstart

Add shiva as a dependency

```shell
go get github.com/jkratz55/shiva
```

### Consumer

### Producer