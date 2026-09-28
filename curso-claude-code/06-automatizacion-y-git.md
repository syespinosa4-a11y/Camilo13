# Módulo 6 — Automatización, Git y trabajo en paralelo

## 6.1 Git con Claude
Claude maneja git muy bien. Pídeselo en lenguaje natural:
- "¿Qué cambió en los últimos 5 commits y por qué?"
- "Haz commit de esto con un buen mensaje."
- "Crea una rama, sube los cambios y abre un PR con descripción." (con `gh` o la integración de GitHub)
- "Resuelve los conflictos del merge con main."

## 6.2 Modo headless (`-p`): Claude en scripts
```bash
claude -p "resume los cambios de git diff en 3 viñetas"
git diff | claude -p "¿hay algún bug en este diff?"
claude -p "arregla los tests fallidos" --allowedTools "Edit,Bash(python3 -m unittest:*)"
claude -p "lista los TODO del repo" --output-format json
```
Úsalo en CI, pre-commit, cron, o para procesar muchos archivos en un bucle:
```bash
for f in tienda/*.py; do claude -p "añade docstrings a $f sin cambiar la lógica" --allowedTools Edit; done
```

## 6.3 GitHub
- Con la GitHub App/Action puedes mencionar `@claude` en issues y PRs para que implemente o revise.
- Configúralo desde la CLI con `/install-github-app`.

## 6.4 Trabajo en paralelo
- **Git worktrees**: varias copias de trabajo del repo, una sesión de Claude en cada una.
  ```bash
  git worktree add ../tienda-feature-a -b feature-a
  cd ../tienda-feature-a && claude
  ```
- **Sesiones en la nube** (claude.ai/code): lanza varias tareas, revísalas luego (incluso desde el móvil).
- Patrón escritor/revisor: una sesión implementa, otra (contexto limpio) revisa.

## 6.5 Hábitos de experto (resumen del curso)
1. Una tarea por conversación; `/clear` a menudo.
2. CLAUDE.md corto y vivo.
3. Planifica antes de cambios grandes (modo plan).
4. Dale siempre un modo de verificar (tests, comandos, capturas).
5. Interrumpe pronto (`Esc`), corrige, sigue.
6. Convierte lo repetido en skills; lo obligatorio en hooks.
7. Delega exploración a subagentes para cuidar el contexto.
8. Revisa siempre el diff antes de hacer merge. Tú eres el responsable del código.

➡️ **Lab 8** (proyecto final).
