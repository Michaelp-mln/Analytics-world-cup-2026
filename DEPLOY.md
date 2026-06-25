# 🚀 Deploy via GitHub Actions → VPS

CI/CD: a cada `git push` na `main`, o **GitHub Actions** builda a imagem,
publica no **GHCR** e faz deploy no seu **VPS** por SSH. O **Caddy** expõe a
aplicação com HTTPS automático; Postgres e Kafka ficam só na rede interna.

```
git push → Actions (build) → GHCR (imagem) → SSH no VPS → docker compose up -d
```

---

## 1. Pré-requisitos no VPS (uma vez)

Numa VM Ubuntu (ex.: Oracle Cloud free / Hetzner), com **≥ 4 GB RAM**:

```bash
# Docker + plugin compose
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER && newgrp docker

# Clonar o projeto em ~/app
git clone https://github.com/SEU_USUARIO/SEU_REPO.git ~/app
cd ~/app

# Configurar variáveis
cp .env.prod.example .env
nano .env          # defina IMAGE, DOMAIN, POSTGRES_PASSWORD
```

Se o repositório (e a imagem GHCR) for **privado**, autentique o servidor uma vez
com um Personal Access Token com escopo `read:packages`:

```bash
echo SEU_PAT | docker login ghcr.io -u SEU_USUARIO --password-stdin
```
> Se tornar o *package* público em GitHub → Packages → Package settings, esse
> passo é dispensável.

### Subir pela primeira vez
```bash
docker compose -f docker-compose.prod.yml up -d
```
Acesse `http://IP_DO_SERVIDOR` (ou `https://seu-dominio` se configurou `DOMAIN`).

---

## 2. Secrets no GitHub (uma vez)

No repositório: **Settings → Secrets and variables → Actions → New secret**:

| Secret | Valor |
|---|---|
| `SSH_HOST` | IP do VPS |
| `SSH_USER` | usuário SSH (ex.: `ubuntu`) |
| `SSH_KEY` | chave **privada** SSH com acesso ao VPS |

Gere o par de chaves (se não tiver) e autorize no servidor:
```bash
ssh-keygen -t ed25519 -f deploy_key -N ""
ssh-copy-id -i deploy_key.pub usuario@IP_DO_SERVIDOR
# cole o conteúdo de `deploy_key` (privada) no secret SSH_KEY
```

---

## 3. Domínio + HTTPS (opcional, recomendado)

1. Aponte um registro **A** do seu domínio para o IP do VPS.
2. No `.env` do servidor: `DOMAIN=copa2026.seudominio.com`
3. `docker compose -f docker-compose.prod.yml up -d`

O Caddy emite e renova o certificado TLS automaticamente. Sem domínio, mantenha
`DOMAIN=:80` (acesso via IP, sem HTTPS).

---

## 4. Deploy automático

A partir daqui, é só:
```bash
git push        # na branch main
```
O Actions builda, publica no GHCR e atualiza o VPS sozinho. Acompanhe na aba
**Actions** do GitHub. Para disparar manualmente: **Actions → build-and-deploy → Run workflow**.

---

## 5. Firewall (recomendado)

Abra **apenas** 22 (SSH), 80 e 443:
```bash
sudo ufw allow 22,80,443/tcp && sudo ufw enable
```
Postgres (5432) e Kafka (9092) **não** ficam expostos nesta configuração.

---

## 🔒 Segurança — verifique já

O arquivo `.env` local contém a sua **chave da API-Football**. Confirme que ele
**não foi enviado ao GitHub** (ele está no `.gitignore`):

```bash
git ls-files | grep -E "^\.env$"     # não deve retornar nada
```
Se aparecer, remova do histórico (`git rm --cached .env`) e **revogue/rode a chave**.

---

## Operação no servidor

```bash
docker compose -f docker-compose.prod.yml ps        # status
docker compose -f docker-compose.prod.yml logs -f api
docker compose -f docker-compose.prod.yml down      # parar (mantém dados)
```

> ⚠️ Com a URL pública, qualquer um pode chamar `POST /api/sim/reset` e
> `/api/sim/next-round`. Se for expor para o público, proteja esses endpoints
> (ex.: basic auth no Caddy) ou desabilite o controle manual.
