#!/bin/sh

for cert in certs/*_cert.der; do
    openssl x509 -inform DER -in "$cert" -outform PEM -out "$cert".pem
done

for cert in certs/*_cert.der.pem; do
    openssl verify -CAfile certs/ca_cert.der.pem -verify_hostname "$(basename "$cert" _cert.der.pem)" -policy_check -x509_strict "$cert"
done
