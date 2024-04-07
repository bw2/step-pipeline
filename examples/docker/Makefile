IMAGE_NAME = step-pipeline-examples
TAG = weisburd/${IMAGE_NAME}:latest

all: build push

build:
	docker build -t $(TAG) .

push:
	docker push $(TAG)
	docker push $(TAG) | tee /dev/tty | grep sha256 | grep latest | cut -d ' ' -f 3 > sha256.txt
	cat sha256.txt && [ ! -z "`cat sha256.txt`" ] && sed -i.bak "s/${IMAGE_NAME}@sha256:[^\"]*/${IMAGE_NAME}"@`cat sha256.txt`"/"  ../*.py