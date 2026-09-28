# Módulo 5 — Subagentes y MCP

## 5.1 Subagentes
Un subagente es otra instancia de Claude con **su propio contexto limpio**, que hace una tarea y devuelve solo el resultado.
Ventajas: no ensucia tu contexto principal, puede ir en paralelo, puede tener herramientas limitadas.

Usos:
- "Usa un subagente para investigar cómo se usan los descuentos en todo el repo y dame solo un resumen."
- "Lanza 3 subagentes en paralelo: uno revisa seguridad, otro rendimiento, otro legibilidad."

Crear el tuyo: `/agents` (asistente interactivo) o un archivo `.claude/agents/revisor.md`:
```markdown
---
name: revisor
description: Revisor de código exigente. Úsalo tras terminar un cambio para detectar bugs.
tools: Read, Grep, Glob, Bash
---
Eres un revisor senior. Revisa `git diff` y reporta solo problemas reales:
bugs, casos límite sin cubrir, tests que faltan. Ordena por gravedad. No edites archivos.
```

## 5.2 MCP (Model Context Protocol)
Conecta Claude Code a herramientas externas: GitHub, bases de datos, navegador, Jira/Linear, Slack, Figma, Sentry…
```bash
claude mcp add <nombre> -- <comando para arrancar el servidor>   # servidor local (stdio)
claude mcp add --transport http <nombre> <url>                  # servidor remoto
claude mcp list
```
- Dentro de la sesión: `/mcp` para ver estado y autenticarte.
- `.mcp.json` en la raíz del proyecto comparte servidores con el equipo.
- Cada servidor añade herramientas al contexto: **conecta solo lo que uses**.

## 5.3 Plugins
Los plugins empaquetan skills, subagentes, hooks y servidores MCP para instalarlos de una vez (`/plugin`).
Útiles para compartir la configuración de un equipo.

➡️ **Lab 7**.
