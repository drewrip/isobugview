.DEFAULT_GOAL := build-docker

IMAGENAME := isodiff-server
EXTPORT := 8000
INPORT := 8000

build-docker:
	docker build -t $(IMAGENAME) .

run-docker:
	docker run --rm -it --name $(IMAGENAME) -p $(EXTPORT):$(INPORT) $(IMAGENAME)
