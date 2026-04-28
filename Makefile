.PHONY: test 

test:
	go test ./...

clients/ts/dist/hrana.bundle.js:
	cd clients/ts && npm run bundle
