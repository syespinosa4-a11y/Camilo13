# Curso corto: domina Claude Code

Curso práctico en 6 módulos + laboratorio. Duración estimada: **4–6 horas** (puedes hacerlo en 3 sesiones).

| # | Módulo | Qué aprendes | Lab |
|---|--------|--------------|-----|
| 1 | [Fundamentos](01-fundamentos.md) | Instalar, abrir sesión, comandos `/`, `@`, `!`, atajos | Lab 1 |
| 2 | [Contexto y memoria](02-contexto-y-memoria.md) | `CLAUDE.md`, `/init`, `/context`, `/compact`, `/clear` | Lab 2 |
| 3 | [Flujo de trabajo pro](03-flujo-de-trabajo.md) | Explorar → Planificar → Codificar → Verificar, modo plan, rewind | Labs 3–4 |
| 4 | [Personalización](04-personalizacion.md) | Permisos, `settings.json`, skills/comandos propios, hooks | Labs 5–6 |
| 5 | [Subagentes y MCP](05-subagentes-y-mcp.md) | Delegar tareas, conectar herramientas externas | Lab 7 |
| 6 | [Automatización y Git](06-automatizacion-y-git.md) | Modo headless `-p`, commits/PRs, GitHub, sesiones paralelas | Lab 8 |

📋 [Chuleta (cheatsheet)](chuleta.md) · 🧪 [Laboratorio](laboratorio/README.md)

## Cómo usar este curso

1. Lee un módulo (10–15 min cada uno).
2. Haz el lab correspondiente **dentro de Claude Code**, trabajando sobre `laboratorio/`.
3. Al final de cada lab hay una sección **"Comprueba"**: si la cumples, pasas al siguiente.

> Consejo: crea una rama por lab (`git switch -c lab-1`) para poder repetirlo cuando quieras.

## La idea central (léela dos veces)

Claude Code no es un autocompletado: es un **agente** que lee archivos, ejecuta comandos
y edita código en un bucle. Tu trabajo pasa de "escribir código" a:

1. **Dar buen contexto** (qué, dónde, por qué, cómo se verifica).
2. **Darle una forma de verificarse** (tests, linter, capturas, un comando que diga "funciona").
3. **Revisar y corregir el rumbo** pronto (interrumpir con `Esc` es barato; deshacer una hora de trabajo no).

Si solo te quedas con una cosa: **Claude trabaja mucho mejor cuando puede comprobar su propio trabajo.**
