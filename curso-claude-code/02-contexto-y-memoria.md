# Módulo 2 — Contexto y memoria

## 2.1 La ventana de contexto es tu recurso más valioso
Todo lo que Claude lee (archivos, salidas de comandos, tu conversación) ocupa contexto.
Cuanto más lleno y ruidoso, peor rinde. Gestionarlo es **la** habilidad avanzada.

| Comando | Cuándo usarlo |
|---|---|
| `/context` | Ver qué está ocupando el contexto |
| `/clear` | Al cambiar de tarea. Hazlo mucho. |
| `/compact [instrucciones]` | Resumir la conversación y seguir. Ej: `/compact conserva la lista de bugs y los archivos tocados` |

Regla práctica: **una tarea = una conversación**. Si llevas mucho rato corrigiendo a Claude en la misma dirección, `/clear` y empieza con un prompt mejor que incluya lo aprendido.

## 2.2 CLAUDE.md: la memoria del proyecto
Archivo Markdown que Claude carga automáticamente al empezar cada sesión.

| Ubicación | Alcance |
|---|---|
| `./CLAUDE.md` | Proyecto, se sube a git (compartido con el equipo) |
| `./CLAUDE.local.md` | Proyecto, solo para ti (añádelo a `.gitignore`) |
| `~/.claude/CLAUDE.md` | Todas tus sesiones en tu máquina |
| `subcarpeta/CLAUDE.md` | Se carga cuando Claude trabaja en esa carpeta |

Qué poner (corto y concreto):
```markdown
# Proyecto Tienda
## Comandos
- Tests: `python3 -m unittest -v` (desde laboratorio/)
## Convenciones
- Python 3.11, solo biblioteca estándar. Nombres en español.
- Nunca modificar tests para que pasen: arreglar el código.
## Arquitectura
- tienda/inventario.py: stock y precios · carrito.py: compra · descuentos.py: cupones
```
Qué **no** poner: documentación larga, cosas obvias, secretos.
Puedes importar otros archivos con `@ruta/archivo.md` dentro de CLAUDE.md.

- `/init` genera un CLAUDE.md inicial analizando el repo. Luego **edítalo tú**: recórtalo.
- `/memory` abre los archivos de memoria para editarlos.
- Tip: cuando Claude repita un error, dile "añade a CLAUDE.md una regla para que no vuelva a pasar".

## 2.3 Dar contexto de forma eficiente
- Usa `@archivo` en vez de "busca el archivo del carrito".
- Pega el error completo o una captura (`Ctrl+V`).
- Da URLs de documentación si la librería es nueva/rara.
- Para preguntas amplias ("¿cómo funciona la autenticación?") deja que Claude explore: lo hará con búsquedas y, si hace falta, subagentes (módulo 5).

➡️ **Lab 2**.
