setup:
	go mod download
	go install github.com/ahmetb/govvv

test:
	./test_compile

format:
	goimports -w -l `find . -type f -name '*.go' -not -path './vendor/*'`

lint:
	echo 'Linting with prettier...'
	npx prettier --check "./**" 2> /dev/null || true
	echo 'Linting with golint...'
	golint `go list ./... | grep -v /vendor/`

textile:
	$(eval FLAGS := $$(shell govvv -flags | sed 's/main/github.com\/SJTU-OpenNetwork\/hon-textile\/common/g'))
	go install -ldflags "-w $(FLAGS)" github.com/SJTU-OpenNetwork/hon-textile/cmd/textile

textile-win:
	$(eval FLAGS := $$(shell govvv -flags | sed 's/main/github.com\/SJTU-OpenNetwork\/hon-textile\/common/g'))
	CGO_ENABLED=1 GOOS=windows GOARCH=amd64 CC=x86_64-w64-mingw32-gcc go build -ldflags "-linkmode external -extldflags -static -s -w $(FLAGS)" -o $(GOPATH)/bin/textile-win.exe github.com/SJTU-OpenNetwork/hon-textile/cmd/textile

ios:
	$(eval FLAGS := $$(shell govvv -flags | sed 's/main/github.com\/SJTU-OpenNetwork\/hon-textile\/common/g'))
	env go111module=off gomobile bind -ldflags "-w $(FLAGS)" -v -target=ios github.com/SJTU-OpenNetwork/hon-textile/mobile github.com/SJTU-OpenNetwork/hon-textile/core
	mkdir -p mobile/dist/ios/ && cp -r Mobile.framework mobile/dist/ios/
	rm -rf Mobile.framework

android:
	$(eval FLAGS := $$(shell govvv -flags | sed 's/main/github.com\/SJTU-OpenNetwork\/hon-textile\/common/g'))
	env go111module=off gomobile bind -ldflags "-w $(FLAGS)" -v -target=android -o mobile.aar github.com/SJTU-OpenNetwork/hon-textile/mobile github.com/SJTU-OpenNetwork/hon-textile/core
	mkdir -p mobile/dist/android/textile2 && mv mobile.aar mobile/dist/android/textile2

protos:
	$(eval P_TIMESTAMP := Mgoogle/protobuf/timestamp.proto=github.com/golang/protobuf/ptypes/timestamp)
	$(eval P_ANY := Mgoogle/protobuf/any.proto=github.com/golang/protobuf/ptypes/any)
	$(eval PKGMAP := $$(P_TIMESTAMP),$$(P_ANY))
	cd pb/protos; protoc --go_out=$(PKGMAP):.. *.proto; protoc --java_out=../java *.proto

.PHONY: docs
docs:
	go get github.com/swaggo/swag/cmd/swag
	swag init -g core/api.go
	npm i -g swagger-markdown
	swagger-markdown -i docs/swagger.yaml -o docs/swagger.md

docker:
	$(eval VERSION := $$(shell ggrep -oP 'const Version = "\K[^"]+' common/version.go))
	docker build -t go-textile:$(VERSION) .

docker_cafe:
	$(eval VERSION := $$(shell ggrep -oP 'const Version = "\K[^"]+' common/version.go))
	docker build -t go-textile:$(VERSION)-cafe -f Dockerfile.cafe .
