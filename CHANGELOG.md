# Changelog

## [0.1.5](https://github.com/octarhq/octar/compare/v0.1.4...v0.1.5) (2026-05-27)


### ✨ Features

* add workflow dispatch input for Docker build and trigger Docker workflow on new tag ([bf73a91](https://github.com/octarhq/octar/commit/bf73a9149d7ebdbca7d44c551f73b5ff622ef83d))

## [0.1.4](https://github.com/octarhq/octar/compare/v0.1.3...v0.1.4) (2026-05-27)


### 🐛 Bug Fixes

* increase timeout for handler calls in TCP server tests to improve reliability ([ed019ba](https://github.com/octarhq/octar/commit/ed019ba5bc12b8b619ef86710a63f7abd92bc61a))

## [0.1.3](https://github.com/octarhq/octar/compare/v0.1.2...v0.1.3) (2026-05-26)


### 🐛 Bug Fixes

* correct WAL ChannelFull test race — stop loop before swapping channel ([a6c9188](https://github.com/octarhq/octar/commit/a6c91886b552a88116a22d4b38d8a9a8246e822a))
* extend TCP server test timeouts to survive bcrypt on slow CI runners ([ee33a85](https://github.com/octarhq/octar/commit/ee33a852ab9e58d74351500ecb46f496eb6d30d5))
* resolve data races and flaky test timings detected by -race on Linux ([32fb7bd](https://github.com/octarhq/octar/commit/32fb7bddb22babff46145958e218818705a43089))

## [0.1.2](https://github.com/octarhq/octar/compare/v0.1.1...v0.1.2) (2026-05-26)


### 🐛 Bug Fixes

* resolve data races and golangci-lint config ([db94597](https://github.com/octarhq/octar/commit/db945970df3b77d336f5f42653c6f0aa510a153c))

## [0.1.1](https://github.com/octarhq/octar/compare/v0.1.0...v0.1.1) (2026-05-26)


### ✨ Features

* wildcard subscribe + CI/CD pipelines ([eadae63](https://github.com/octarhq/octar/commit/eadae6307fd2a6588285af09f138f33edabb0003))


### 🐛 Bug Fixes

* add Linux freeDiskSpace impl and fix golangci-lint Go version ([4e315ff](https://github.com/octarhq/octar/commit/4e315ffb4e33be3bd6bce05d0a7fcb99bf563286))
* golangci-lint v2 config schema corrections ([2ed2667](https://github.com/octarhq/octar/commit/2ed26679ac3b94b1e4de9d58c403e0d65daf3043))
* resolve all errcheck violations and ineffectual assignments in queue_test.go ([404d868](https://github.com/octarhq/octar/commit/404d868f7b51498cd2736c8bc5d95d3fb6b8886a))
* use RELEASE_PLEASE_TOKEN PAT for PR creation ([5507de2](https://github.com/octarhq/octar/commit/5507de26c6dd55ab4d48caa20b4c75b75ca9738c))
