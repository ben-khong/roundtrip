# Roundtrip

A ride-sharing platform powered by a Go microservices backend, running on Docker and Kubernetes.

## Overview

Roundtrip is a backend system for a ride-sharing application, built as a set of independently deployable Go microservices. It includes an API gateway, driver and trip management, payment processing, async messaging via RabbitMQ, and distributed tracing via Jaeger. The system is designed to be horizontally scalable and deployable to a Kubernetes cluster, either locally (Minikube / Docker Desktop) or in the cloud (example: Google Kubernetes Engine).

## Architecture

**Services**
- `api-gateway` – entry point for client requests, routes to internal services
- `driver-service` – manages driver state and availability
- `trip-service` – handles trip scheduling and lifecycle
- `payment-service` – handles payment processing

**Infrastructure**
- RabbitMQ – async messaging between services
- Jaeger – distributed tracing

### Trip scheduling flow

[![Trip scheduling flow](https://mermaid.ink/img/pako:eNqNVt9v2jAQ_lcsP21qGvGjZSEPlSpaTX1YxWDVpAmpMvZBIkicOQ6UVf3fd4mdEgcKzQOK47v7vjt_d-aVcimAhnSW5vC3gJTDXcyWiiWzlOCTMaVjHmcs1eQpB3X49Xb88J1p2LLd4d4vFWdTUJuYw-HmnYo3oM5sH34fs10Cqf7Qb6oRFL-bnZLz5c3NnmRIRgrwteJGJmXOuTa2eyP0aFAPyXIyHtWO5YaxN7-PEoNJpNrM1nOSCw3Y_QuPWLq0nBvWl4h30fIos_Bhg5n6vMIVDTgVLyNN5II4LO9L65A8wrbyJimAyAkjwlbSBHBwENisQ2vl80T4pfezapbGRW1RHckkYaloILMNi9dsvgYXtHUQv2E-lXwF-hCccQ5ZE3sNiwZ0A_O2sjSw6oPTbB_nWbT3TB3dGEiykGrLlABBtCQTNp_H-sdP8sUwK3EmkGcS2-lnAQV8rUtwVCchGSvJIc8tJ2KoMGxDjxSZKIVapZZrpovc9_2j4nF7whGPifvM8jxepp8XkW2SzAQmOVKMZXoyF6_Nwq5bunetkLxp2HfIUQR8JQts5Bqz9DJGx3K1ZtZdHAVpCc9mZStkc3s-0WZtTFukOsGnB5QeE7u6PM4gKScQsozktmF_TmuN1qidUHZJa6q9V04m2RowlrV1SnbQc5GUq33YacFL_R2faHtPzxXt0ZN1O-6iXbRW1Zu4HxeiqjTJivk6ziPTcp-U1aXD-NPgZ46a21KL061Qj6kzc38_facarzCyv1tOdGgVk7OUpCipOSxjZEI9moBKWCzwKn8tQ8yojiCBGQ3xVTC1muEV_4Z2rNByuks5DbUqwKNKFsuIhgu2znFlZo79C1Cb4L36R8rmkoav9IWGvW_-1XVn0O_1-kE3GAyHgUd3-Lnb8fu9frc_xKfbvQ6CN4_-qyJ0_KDX7Q86QTDoDAfD66ve23_1IPGQ?type=png)](https://mermaid.live/edit#pako:eNqNVt9v2jAQ_lcsP21qGvGjZSEPlSpaTX1YxWDVpAmpMvZBIkicOQ6UVf3fd4mdEgcKzQOK47v7vjt_d-aVcimAhnSW5vC3gJTDXcyWiiWzlOCTMaVjHmcs1eQpB3X49Xb88J1p2LLd4d4vFWdTUJuYw-HmnYo3oM5sH34fs10Cqf7Qb6oRFL-bnZLz5c3NnmRIRgrwteJGJmXOuTa2eyP0aFAPyXIyHtWO5YaxN7-PEoNJpNrM1nOSCw3Y_QuPWLq0nBvWl4h30fIos_Bhg5n6vMIVDTgVLyNN5II4LO9L65A8wrbyJimAyAkjwlbSBHBwENisQ2vl80T4pfezapbGRW1RHckkYaloILMNi9dsvgYXtHUQv2E-lXwF-hCccQ5ZE3sNiwZ0A_O2sjSw6oPTbB_nWbT3TB3dGEiykGrLlABBtCQTNp_H-sdP8sUwK3EmkGcS2-lnAQV8rUtwVCchGSvJIc8tJ2KoMGxDjxSZKIVapZZrpovc9_2j4nF7whGPifvM8jxepp8XkW2SzAQmOVKMZXoyF6_Nwq5bunetkLxp2HfIUQR8JQts5Bqz9DJGx3K1ZtZdHAVpCc9mZStkc3s-0WZtTFukOsGnB5QeE7u6PM4gKScQsozktmF_TmuN1qidUHZJa6q9V04m2RowlrV1SnbQc5GUq33YacFL_R2faHtPzxXt0ZN1O-6iXbRW1Zu4HxeiqjTJivk6ziPTcp-U1aXD-NPgZ46a21KL061Qj6kzc38_facarzCyv1tOdGgVk7OUpCipOSxjZEI9moBKWCzwKn8tQ8yojiCBGQ3xVTC1muEV_4Z2rNByuks5DbUqwKNKFsuIhgu2znFlZo79C1Cb4L36R8rmkoav9IWGvW_-1XVn0O_1-kE3GAyHgUd3-Lnb8fu9frc_xKfbvQ6CN4_-qyJ0_KDX7Q86QTDoDAfD66ve23_1IPGQ)

## Tech stack

- **Language:** Go
- **Containers:** Docker
- **Orchestration:** Kubernetes
- **Local dev:** Tilt, Minikube
- **Messaging:** RabbitMQ
- **Tracing:** Jaeger

## Project structure

```
.
├── docs/architecture   # Architecture documentation
├── infra               # Docker/Kubernetes manifests (dev + production)
├── proto               # Protobuf definitions for inter-service communication
├── services            # Go microservices (api-gateway, driver, trip, payment)
├── shared              # Shared Go packages/libraries
├── tools               # Developer tooling/scripts
├── web                 # Frontend client
├── Makefile
├── Tiltfile
├── go.mod
└── go.sum
```

## Prerequisites

- [Go](https://go.dev/)
- [Docker](https://www.docker.com/products/docker-desktop/)
- [Tilt](https://tilt.dev/)
- A local Kubernetes cluster ([Minikube](https://minikube.sigs.k8s.io/docs/) or Docker Desktop's built-in cluster)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

### macOS setup

```bash
# Install Homebrew: https://brew.sh/

# Install Docker Desktop: https://www.docker.com/products/docker-desktop/
# Install Minikube: https://minikube.sigs.k8s.io/docs/
# Install Tilt: https://tilt.dev/

# Install Go
brew install go
```

### Windows (WSL) setup

Install [WSL](https://learn.microsoft.com/en-us/windows/wsl/install), then [Docker Desktop](https://www.docker.com/products/docker-desktop/), [Minikube](https://minikube.sigs.k8s.io/docs/), and [Tilt](https://tilt.dev/). Install Go inside WSL:

```bash
wget https://dl.google.com/go/go1.23.0.linux-amd64.tar.gz
sudo tar -xvf go1.23.0.linux-amd64.tar.gz
sudo mv go /usr/local

# Add to ~/.bashrc
export GOROOT=/usr/local/go
export GOPATH=$HOME/go
export PATH=$GOPATH/bin:$GOROOT/bin:$PATH

go version
```

Install `kubectl` from the [official docs](https://kubernetes.io/docs/tasks/tools/).

## Getting started

Start the local development environment:

```bash
tilt up
```

Monitor running pods:

```bash
kubectl get pods
```

or open the dashboard:

```bash
minikube dashboard
```

## Deployment

The example below deploys to Google Kubernetes Engine (GKE). Run these steps manually first, then adapt them into a CI/CD pipeline for your infrastructure.

### 1. Set environment variables

```bash
REGION=europe-west1       # change to your region
PROJECT_ID=roundtrip-503423
```

### 2. Add production secrets

Add a `secrets.yaml` file to the production folder. You can copy the development secrets as a starting point.

### 3. Build the Docker images

```bash
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/roundtrip/api-gateway:latest \
  --platform linux/amd64 -f infra/production/docker/api-gateway.Dockerfile .

docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/roundtrip/driver-service:latest \
  --platform linux/amd64 -f infra/production/docker/driver-service.Dockerfile .

docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/roundtrip/trip-service:latest \
  --platform linux/amd64 -f infra/production/docker/trip-service.Dockerfile .

docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/roundtrip/payment-service:latest \
  --platform linux/amd64 -f infra/production/docker/payment-service.Dockerfile .
```

### 4. Create an Artifact Registry repository

In Google Cloud, go to **Artifact Registry** and create a Docker repository to host the images.

### 5. Push the images

```bash
gcloud auth login
gcloud config set project ${PROJECT_ID}
gcloud auth configure-docker ${REGION}-docker.pkg.dev
```

Then push each image built in step 3. See the [Artifact Registry docs](https://cloud.google.com/artifact-registry/docs/docker/pushing-and-pulling#cred-helper) if you run into authentication errors.

### 6. Create a GKE cluster

Create a cluster via `gcloud` or the Cloud Console UI.

### 7. Apply the Kubernetes manifests

```bash
gcloud container clusters get-credentials roundtrip --region ${REGION} --project ${PROJECT_ID}
```

Apply manifests in order, waiting for each dependency to be ready before continuing:

```bash
# Config and secrets
kubectl apply -f infra/production/k8s/app-config.yaml
kubectl apply -f infra/production/k8s/secrets.yaml

# Infrastructure
kubectl apply -f infra/production/k8s/jaeger-deployment.yaml
kubectl apply -f infra/production/k8s/rabbitmq-deployment.yaml
# Wait for Jaeger and RabbitMQ to be running

# Services
kubectl apply -f infra/production/k8s/api-gateway-deployment.yaml
# Wait for the API gateway to be up, then continue
kubectl apply -f infra/production/k8s/driver-service-deployment.yaml
kubectl apply -f infra/production/k8s/trip-service-deployment.yaml
kubectl apply -f infra/production/k8s/payment-service-deployment.yaml
```

To redeploy:

```bash
kubectl apply -f infra/production/k8s
kubectl rollout restart deployment
```

### 8. Access the API

```bash
kubectl get services
```

Use the external IP listed for `api-gateway`.

### Switching back to local development

```bash
kubectl config get-contexts

# Docker Desktop
kubectl config use-context docker-desktop

# Minikube
kubectl config use-context minikube
```

## Adding HTTPS

1. Reserve a static IP in GCP (VPC Network → External IP addresses), named e.g. `api-gateway-ip`, in the same region as your cluster (or "global" for a global Ingress). Confirm with:

   ```bash
   gcloud compute addresses list
   ```

2. Add an Ingress resource and change the API gateway service from `LoadBalancer` to `ClusterIP`.

3. Apply the config:

   ```bash
   kubectl apply -f infra/production/k8s/api-gateway-ingress.yaml
   kubectl apply -f infra/production/k8s/api-gateway-deployment.yaml
   ```

4. Get the Ingress IP:

   ```bash
   kubectl get ingress api-gateway-ingress
   ```

5. Wait for the Google-managed SSL certificate to provision:

   ```bash
   kubectl describe managedcertificate api-gateway-cert
   ```

   Once the status changes from "Provisioning" to "Active", the API is reachable at `https://<IP_ADDRESS>`.

   > Note: with a self-signed/managed certificate on a bare IP, browsers may show a security warning. For production, use a proper domain name.
