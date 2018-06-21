#!/usr/bin/env bash
ISTIOCTL=$(which istioctl)
if [ "$ISTIOCTL" == "" ]; then
	echo "istioctl does not exist in the path!"
	exit 1
fi

yamls_under_istio=(
  booking.yaml
  car.yaml
  hotel.yaml
)

yamls_without_istio=(
  alpha.yaml
  postgresql.yaml
)

# First make sure namespace is there
# kubectl apply -f namespace.yaml

for yamlfile in "${yamls_without_istio[@]}"
do
	echo $yamlfile
	kubectl apply -f ./${yamlfile}
done

for yamlfile in "${yamls_under_istio[@]}"
do
	echo $yamlfile
	$ISTIOCTL kube-inject -f ./${yamlfile} | kubectl apply -f -
done
