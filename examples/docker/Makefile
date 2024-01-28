TAG = weisburd/step-pipeline-examples:latest

all: build push

build:
	docker build -t $(TAG) .

push:
	docker push $(TAG)
