# Makefile for easy testing

test: all

all: test-background_task \
	test-blacklist \
	test-limits \
	test-notification_bus

test-background_task:
	@./node_modules/.bin/mocha test/test-background_task.js

test-blacklist:
	@./node_modules/.bin/mocha test/test-blacklist.js

test-limits:
	@./node_modules/.bin/mocha test/test-limits.js

test-notification_bus:
	@./node_modules/.bin/mocha test/test-notification_bus.js

.PHONY: test
