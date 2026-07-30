# Roundtrip

A ride-sharing platform powered by a Go microservices backend, running on Docker and Kubernetes.

## Overview

Roundtrip is a backend system for a ride-sharing application, built as a set of independently deployable Go microservices. It includes an API gateway, driver and trip management, payment processing, async messaging via Kafka, and distributed tracing via Jaeger. The system is designed to be horizontally scalable and deployable to a Kubernetes cluster, either locally (Minikube / Docker Desktop) or in the cloud (example: Google Kubernetes Engine).

## Architecture

**Services**
- `api-gateway` – entry point for client requests, routes to internal services
- `driver-service` – manages driver state and availability
- `trip-service` – handles trip scheduling and lifecycle
- `payment-service` – handles payment processing

**Infrastructure**
- Kafka – async messaging between services
- Jaeger – distributed tracing

### Trip scheduling flow

[![Trip scheduling flow](https://mermaid.ink/img/pako:eNqNVW1r2zAQ_itCnzbqmry0meMPhZKOUQYlLCuDESiKfElEYsuT5WRZ6X_f2ZIbyXmrPxhbeu6eu9Nzp1fKZQI0ptOsgD8lZBweBFsolk4zgk_OlBZc5CzT5LkAdbh6P378xjRs2e5w76cS-QTURnA43HxQYgPqwvbh-pjtUsj0SbuJRlJcNztVzNd3d_sgYzJSgJ91bORHlXOhDXYPQgsn9JgsfoxHjWG1YfDm_STRmcRQ3WwDL7mYfGfzFTN4B3WNPFctZBV9CBvMMOQ1X-LQKLFYaiLnxIvua4WOyRNsa2uSASQFYSSxFTQOPB4kNv-xRYU8TcLK-kW5JfFZW6GOZJqyLHGY2YaJNZutwSdtHcAvmE0kX4E-JGecQ-5yr2HuUDuc9zXS0KoTp9g-xots75l6ejGUZC7VlqkEEqKlOVDy6bhHBOSCf26KcFQhMRkryaEobFTEmDJswICUeVJJtE6u0EyXRRiGR-Xjd4MnH-P3hRWFWGQfl5Ftj9w4JgWGKGR2NpegHYUjd38LIzb9-c4zWgJfyRL7tiGqrAzoWIIWZs2ToyQtvdlUbFlsQi9nuqvNaStjl8nzIyqOJbumJt7cqAYORrmUWwd_SWJORzRGqLa0NcTeKyfTfA3oy2K9kh202lLK1d7tpOSV6I4PsL1l4CnDOU6_u67alWqV2iU7nX1dXpKXs7Uolk57nRDQtRfbhxkvHCq3RU3OK72ZQycG-n6sTjTeSWR_WZxpvPom4iwjGYpmBguBEdCApqBSJhK8m18rF1Oql5DClMb4mTC1muKd_YY4Vmo52WWcxlqVEFAly8WSxnO2LvDPjBJ7pzcQvCh_S-n-0viV_qVx70t4c9sZ9Hu9ftSNBsNhFNAdLnc7Yb_X7_aH-HS7t1H0FtB_tYdOGPW6_UEnigad4WB4e9N7-w8C8t81?type=png)](https://mermaid.live/edit#pako:eNqNVW1r2zAQ_itCnzbqmry0meMPhZKOUQYlLCuDESiKfElEYsuT5WRZ6X_f2ZIbyXmrPxhbeu6eu9Nzp1fKZQI0ptOsgD8lZBweBFsolk4zgk_OlBZc5CzT5LkAdbh6P378xjRs2e5w76cS-QTURnA43HxQYgPqwvbh-pjtUsj0SbuJRlJcNztVzNd3d_sgYzJSgJ91bORHlXOhDXYPQgsn9JgsfoxHjWG1YfDm_STRmcRQ3WwDL7mYfGfzFTN4B3WNPFctZBV9CBvMMOQ1X-LQKLFYaiLnxIvua4WOyRNsa2uSASQFYSSxFTQOPB4kNv-xRYU8TcLK-kW5JfFZW6GOZJqyLHGY2YaJNZutwSdtHcAvmE0kX4E-JGecQ-5yr2HuUDuc9zXS0KoTp9g-xots75l6ejGUZC7VlqkEEqKlOVDy6bhHBOSCf26KcFQhMRkryaEobFTEmDJswICUeVJJtE6u0EyXRRiGR-Xjd4MnH-P3hRWFWGQfl5Ftj9w4JgWGKGR2NpegHYUjd38LIzb9-c4zWgJfyRL7tiGqrAzoWIIWZs2ToyQtvdlUbFlsQi9nuqvNaStjl8nzIyqOJbumJt7cqAYORrmUWwd_SWJORzRGqLa0NcTeKyfTfA3oy2K9kh202lLK1d7tpOSV6I4PsL1l4CnDOU6_u67alWqV2iU7nX1dXpKXs7Uolk57nRDQtRfbhxkvHCq3RU3OK72ZQycG-n6sTjTeSWR_WZxpvPom4iwjGYpmBguBEdCApqBSJhK8m18rF1Oql5DClMb4mTC1muKd_YY4Vmo52WWcxlqVEFAly8WSxnO2LvDPjBJ7pzcQvCh_S-n-0viV_qVx70t4c9sZ9Hu9ftSNBsNhFNAdLnc7Yb_X7_aH-HS7t1H0FtB_tYdOGPW6_UEnigad4WB4e9N7-w8C8t81)

## Tech stack

- **Language:** Go
- **Containers:** Docker
- **Orchestration:** Kubernetes
- **Local dev:** Tilt, Minikube
- **Messaging:** Kafka
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
kubectl apply -f infra/production/k8s/kafka-deployment.yaml
# Wait for Jaeger and Kafka to be running

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
