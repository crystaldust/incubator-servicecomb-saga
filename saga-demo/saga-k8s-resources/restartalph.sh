#!/bin/bash
kubectl delete -f ./base/alpha.deployment.yaml
kubectl apply -f ./base/alpha.deployment.yaml

kubectl delete -f ./spring-demo/booking.deployment.yaml
kubectl apply -f ./spring-demo/booking.deployment.yaml
