# Módulo 4 — Personalización: permisos, skills y hooks

## 4.1 Permisos y settings
Jerarquía de configuración:
- `~/.claude/settings.json` — tuya, global
- `.claude/settings.json` — del proyecto (a git)
- `.claude/settings.local.json` — del proyecto, solo tú

```json
{
  "permissions": {
    "allow": ["Bash(python3 -m unittest:*)", "Bash(git status)", "Bash(git diff:*)"],
    "deny":  ["Read(./.env)", "Bash(rm -rf:*)"]
  }
}
```
- `/permissions` para verlos y editarlos de forma interactiva.
- Modos (`Shift+Tab`): normal, aceptar ediciones automáticamente, plan.
- `claude --dangerously-skip-permissions` solo en entornos aislados (contenedores sin datos sensibles).

## 4.2 Skills y comandos propios (prompts reutilizables)
Si repites un prompt, conviértelo en comando. Formato moderno — una *skill*:

`.claude/skills/arregla-tests/SKILL.md`
```markdown
---
name: arregla-tests
description: Ejecuta los tests del laboratorio y arregla los fallos buscando la causa raíz.
---
1. Ejecuta `python3 -m unittest -v` en laboratorio/.
2. Para cada fallo, explica la causa raíz en una frase.
3. Arregla el código (nunca los tests).
4. Repite hasta que todo pase y resume los cambios.
$ARGUMENTS
```
Se invoca con `/arregla-tests`, y Claude también puede usarla sola cuando la `description` encaja con la tarea.
(El formato antiguo `.claude/commands/nombre.md` sigue funcionando para comandos simples.)
`$ARGUMENTS` se sustituye por lo que escribas después del comando.

## 4.3 Hooks: automatismos garantizados
CLAUDE.md es una *sugerencia*; un hook es un *hecho*: el harness ejecuta tu comando en ciertos eventos.

| Evento | Uso típico |
|---|---|
| `PreToolUse` | Bloquear acciones (ej. editar `.env`) |
| `PostToolUse` | Formatear/lint/tests tras cada edición |
| `UserPromptSubmit` | Añadir contexto a cada prompt |
| `Stop` | Al terminar Claude: notificación, verificación final |
| `SessionStart` | Preparar el entorno (instalar dependencias) |

Ejemplo (`.claude/settings.json`): ejecutar tests tras cada edición.
```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Edit|Write",
        "hooks": [
          { "type": "command",
            "command": "cd \"$CLAUDE_PROJECT_DIR/curso-claude-code/laboratorio\" && python3 -m unittest -q 2>&1 | tail -3" }
        ]
      }
    ]
  }
}
```
- El hook recibe JSON por stdin (herramienta, archivo, etc.).
- Código de salida **2** = bloquear/avisar a Claude con lo que escribas en stderr.
- `/hooks` para ver y gestionar hooks. Puedes pedirle a Claude: "configura un hook que…".

➡️ **Labs 5 y 6**.
