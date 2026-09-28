# Chuleta de Claude Code

## Arrancar
| Comando | |
|---|---|
| `claude` | Sesión interactiva |
| `claude "tarea"` | Sesión con prompt inicial |
| `claude -c` / `--continue` | Continuar la última sesión |
| `claude -r` / `--resume` | Elegir sesión a retomar |
| `claude -p "..."` | Modo headless (scripts) |

## Dentro de la sesión
| | |
|---|---|
| `@ruta` | Añadir archivo/carpeta al contexto |
| `!comando` | Ejecutar bash directo |
| `Esc` | Interrumpir |
| `Esc Esc` / `/rewind` | Volver atrás (checkpoints) |
| `Shift+Tab` | Normal → Auto-aceptar ediciones → Plan |
| `Ctrl+V` | Pegar imagen |

## Comandos `/`
| | |
|---|---|
| `/init` | Generar CLAUDE.md |
| `/memory` | Editar memorias |
| `/clear` · `/compact` · `/context` | Gestión de contexto |
| `/model` · `/config` | Modelo y preferencias |
| `/permissions` | Reglas allow/deny |
| `/hooks` · `/agents` · `/mcp` · `/plugin` | Extensiones |
| `/review` · `/security-review` | Revisiones |
| `/cost` · `/usage` | Consumo |
| `/help` | Todo lo demás |

## Archivos
```
CLAUDE.md                     memoria del proyecto
CLAUDE.local.md               memoria personal del proyecto
.claude/settings.json         permisos + hooks (equipo)
.claude/settings.local.json   permisos + hooks (tú)
.claude/skills/<n>/SKILL.md   skills / comandos propios
.claude/agents/<n>.md         subagentes
.mcp.json                     servidores MCP del proyecto
~/.claude/...                 versiones globales de todo lo anterior
```

## Plantilla de prompt
```
Objetivo: ...
Dónde: @archivo, @carpeta
Restricciones: no cambies X, usa Y, sigue el estilo de Z
Verificación: ejecuta `...` y confirma que ...
Primero: explora/propón un plan, no escribas código aún.
```
