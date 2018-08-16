#!/bin/bash
kubectl delete -f ./alpha.deployment.yaml
kubectl apply -f ./alpha.deployment.yaml
