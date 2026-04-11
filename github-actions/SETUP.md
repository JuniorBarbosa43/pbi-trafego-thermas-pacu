# Setup: GitHub Actions → Google Sheets → Power BI

Arquitetura 100% cloud. Nenhum script roda na sua máquina.

---

## 1. Criar o repositório no GitHub

1. Acesse: https://github.com/new
2. Nome: `pbi-trafego-thermas-pacu`
3. Visibilidade: **Private**
4. Marque **Add a README file**
5. .gitignore: **Python**
6. Clique **Create repository**

---

## 2. Fazer upload dos arquivos

Após criar o repositório, faça upload de toda a pasta `github-actions/` para a raiz do repositório.

A estrutura final deve ser:
```
pbi-trafego-thermas-pacu/
├── .github/
│   └── workflows/
│       └── atualizar_dados.yml
├── scripts/
│   ├── sheets_helper.py
│   ├── atualizar_meta_ads_sheets.py
│   ├── atualizar_meta_organico_sheets.py
│   └── atualizar_google_ads_sheets.py
└── requirements.txt
```

**Como fazer o upload:**
1. No repositório, clique em **Add file → Upload files**
2. Arraste a pasta `github-actions/` ou faça upload arquivo por arquivo
3. Commit: "feat: scripts de atualizacao automatica"

---

## 3. Configurar GitHub Secrets

No repositório → **Settings → Secrets and variables → Actions → New repository secret**

Configure **todos** os secrets abaixo:

| Secret | Valor | Onde encontrar |
|--------|-------|----------------|
| `META_TOKEN` | Token do Meta/Facebook | Meta Business Suite → Configurações → Token de acesso |
| `META_PAGE_ID` | ID da página do Facebook | URL da página ou Meta Business |
| `META_IG_ID` | ID da conta do Instagram | Meta Business → Conta do Instagram |
| `META_AD_ACCOUNT_ID` | `act_XXXXXXXX` | Meta Ads Manager → Conta de anúncios |
| `GOOGLE_CLIENT_ID` | `503737325641-vpj812r2bb5jkddd7tpod43lb8lqc8j6.apps.googleusercontent.com` | Google Cloud Console |
| `GOOGLE_CLIENT_SECRET` | `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` | Google Cloud Console |
| `GOOGLE_REFRESH_TOKEN` | *(gerar com gerar_refresh_token_google.py)* | Rodar o script local |
| `GOOGLE_DEVELOPER_TOKEN` | `xr00fFrU4qlg4WlDlkIVfA` | Google Ads Manager → API Center |
| `GOOGLE_CUSTOMER_ID` | `3180978445` | Google Ads → ID da conta |
| `SPREADSHEET_ID` | ID da planilha Google Sheets | URL da planilha (entre /d/ e /edit) |

---

## 4. Criar a planilha Google Sheets

1. Acesse: https://sheets.google.com
2. Crie uma planilha nova: **"Dados Tráfego - Thermas Pacu"**
3. Copie o ID da URL: `https://docs.google.com/spreadsheets/d/**[ID_AQUI]**/edit`
4. Cole esse ID no secret `SPREADSHEET_ID`

As abas serão criadas automaticamente pelos scripts na primeira execução:
- `Meta_Ads_Campanhas`
- `Meta_Organico_FB`
- `Meta_Organico_IG`
- `IG_Posts`
- `Google_Ads_Campanhas`

---

## 5. Gerar o Google Refresh Token

*(Pendente — aguardando senha do Google)*

1. Acesse Google Cloud Console: https://console.cloud.google.com
2. Vá em **APIs → Credenciais → OAuth 2.0 → editar seu client**
3. Em "URIs de redirecionamento autorizados", adicione: `http://localhost:8765/callback`
4. Salve
5. No seu computador, execute:
   ```
   C:\Users\ander\AppData\Local\Python\bin\python.exe scripts\gerar_refresh_token_google.py
   ```
6. Autorize no navegador
7. Copie o `GOOGLE_REFRESH_TOKEN` exibido no terminal
8. Adicione como secret no GitHub

---

## 6. Ativar o Google Sheets API

1. Acesse: https://console.cloud.google.com/apis/library
2. Procure por **"Google Sheets API"**
3. Clique em **Ativar**

*(Provavelmente já está ativo se você usa Google Ads API)*

---

## 7. Conceder acesso à planilha

A planilha precisa ser compartilhada com a conta de serviço OAuth.
Como estamos usando OAuth pessoal (com seu Google), ela já terá acesso.

---

## 8. Testar manualmente

Após configurar os secrets:
1. No repositório → **Actions → Atualizar Dados → Google Sheets**
2. Clique **Run workflow → Run workflow**
3. Aguarde a execução e verifique os logs
4. Confirme que as abas foram criadas na planilha

---

## 9. Conectar Power BI Service ao Google Sheets

1. Abra o Power BI Desktop
2. Em cada tabela que hoje lê JSON local, adicione uma fonte alternativa lendo do Sheets:
   - **Obter Dados → Google Sheets → URL da planilha**
3. Publique o relatório no Power BI Service
4. No Service: **Conjunto de dados → Configurações → Atualização agendada**
5. Configure para atualizar todo dia às **07:00 BRT**

---

## Horários de execução (BRT)

| Script | Horário BRT | Horário UTC (GitHub) |
|--------|-------------|---------------------|
| Meta Ads | 06:00 | 09:00 |
| Meta Orgânico | 06:10 | 09:10 |
| Google Ads | 06:20 | 09:20 |
| Power BI refresh | 07:00 | 10:00 |

---

## Pendências

- [ ] Gerar GOOGLE_REFRESH_TOKEN (aguardando senha do Google)
- [ ] Criar repositório no GitHub
- [ ] Fazer upload dos scripts
- [ ] Configurar todos os secrets
- [ ] Criar planilha e pegar SPREADSHEET_ID
- [ ] Teste manual via "Run workflow"
- [ ] Publicar .pbip no Power BI Service
