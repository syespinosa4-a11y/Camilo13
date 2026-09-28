# Módulo 1 — Fundamentos

## 1.1 Dónde se usa Claude Code
- **Terminal (CLI)**: `claude` dentro de la carpeta del proyecto. Es la experiencia más completa.
- **IDE**: extensiones para VS Code y JetBrains (ves los diffs en el editor).
- **Escritorio y web** (claude.ai/code): sesiones en la nube sobre tus repos de GitHub; útil para lanzar tareas y revisarlas desde el móvil.

Instalación CLI (una de estas):
```bash
npm install -g @anthropic-ai/claude-code   # requiere Node 18+
# o el instalador nativo: consulta https://code.claude.com/docs
claude            # abre sesión interactiva en la carpeta actual
```

## 1.2 Anatomía de una sesión
Escribes en lenguaje natural. Claude decide qué **herramientas** usar (leer, buscar, editar, ejecutar bash…).
Las acciones con riesgo te piden **permiso** — puedes aceptar una vez, aceptar siempre, o rechazar y explicar por qué.

## 1.3 Los tres prefijos mágicos
| Prefijo | Qué hace | Ejemplo |
|---|---|---|
| `/` | Comando (integrado o tuyo) | `/help`, `/clear`, `/model` |
| `@` | Menciona un archivo/carpeta y lo mete al contexto | `explica @tienda/carrito.py` |
| `!` | Ejecuta bash directamente; la salida queda en el contexto | `!python3 -m unittest` |

## 1.4 Atajos que usarás todo el día
| Atajo | Acción |
|---|---|
| `Esc` | Interrumpe a Claude (sin perder lo hecho) — úsalo sin miedo |
| `Esc` `Esc` / `/rewind` | Volver a un punto anterior de la conversación y/o del código |
| `Shift+Tab` | Cambiar modo de permisos: normal → aceptar ediciones → **modo plan** |
| `↑` | Recuperar mensajes anteriores |
| `Ctrl+V` / arrastrar | Pegar una imagen (capturas de errores, diseños) |
| `Ctrl+C` (x2) | Salir |

## 1.5 Comandos básicos
- `/help` — lista todo.
- `/model` — cambia de modelo (más capaz vs. más rápido).
- `/clear` — conversación nueva (el código no se toca).
- `/resume` o `claude --continue` / `claude --resume` — retomar sesiones.
- `/cost` o `/usage` — consumo de la sesión/plan.
- `/config` — preferencias (tema, etc.).

## 1.6 Cómo pedir bien
❌ "arregla los tests"
✅ "Ejecuta `python3 -m unittest` en `laboratorio/`. Hay tests fallando; para cada uno explica la causa raíz antes de tocar código, arregla el código (no los tests) y vuelve a ejecutar hasta que pasen."

Buen prompt = **objetivo + ubicación + restricciones + cómo verificar**.

➡️ Ahora haz el **Lab 1** en [laboratorio/README.md](laboratorio/README.md).
