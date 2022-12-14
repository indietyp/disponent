# Disponent

*disponent* German noun: "manager"/"dispatcher"/"managing clerk".
From the latin word *disponens*.

A *disponent* is someone, who is responsible for the assignment of resources and goods, as
well as the allocation of finances or personnel in an organization.

## Status

This is a highly experimental lightweight crate to manage a distributed job queue, which
currently supports RabbitMQ.

This can be considered pre-alpha.

The following things are missing until a `0.1` release:

* [ ] documentation
* [ ] redis as job-queue
* [ ] async-std integration
* [ ] complete tracing integration
* [ ] return values
* [ ] every/retires
* [ ] memcached as cache
* [ ] testing of multiple worker nodes
* [ ] macro to create a new task from a function

The following is planned for the future (if we ever get there):

* [ ] postgres as backend
* [ ] different possible queues
