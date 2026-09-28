# Laboratorio: la Tienda con bugs 🐛

Proyecto Python pequeño (solo biblioteca estándar) con **bugs sembrados a propósito**.
Lo usarás para practicar cada módulo.

```
laboratorio/
├── tienda/
│   ├── inventario.py   # productos, stock, valor total
│   ├── carrito.py      # carrito de compra
│   └── descuentos.py   # porcentajes y cupones
└── tests/test_tienda.py
```

Ejecutar tests: `python3 -m unittest -v` (desde `laboratorio/`). Al empezar, **4 tests fallan**. Es a propósito.

> Antes de cada lab: `git switch -c lab-N`. Para repetir: `git switch main && git branch -D lab-N`.
> Abre Claude Code en la **raíz del repo** (`claude`).

---

## Lab 1 — Primer contacto (Módulo 1) ⏱ 15 min
1. Abre `claude`. Pregunta: `¿qué hay en este repositorio? explícamelo en 5 líneas`.
2. Usa `@`: `explica @curso-claude-code/laboratorio/tienda/carrito.py línea a línea`.
3. Usa `!`: `!cd curso-claude-code/laboratorio && python3 -m unittest` y luego pregunta `¿por qué fallan esos tests? no arregles nada todavía`.
4. Mientras responde, pulsa `Esc` para interrumpir; luego escribe `sigue, pero más breve`.
5. Prueba `/help`, `/model` y `/cost` (o `/usage`).

**Comprueba:** sabes para qué sirven `/`, `@`, `!` y `Esc`, y Claude te ha explicado los 4 fallos sin editar archivos.

---

## Lab 2 — Memoria del proyecto (Módulo 2) ⏱ 20 min
1. Ejecuta `/init`. Lee el `CLAUDE.md` que genera.
2. Pídele: `recorta CLAUDE.md a menos de 20 líneas: comandos, convenciones y arquitectura`.
3. Añade tú la regla: *"Nunca modificar los tests para que pasen; arreglar el código."*
4. `/clear`. Pregunta `¿cómo ejecuto los tests y qué reglas tengo que seguir?` → debe saberlo sin buscar (lo leyó de CLAUDE.md).
5. Ejecuta `/context` y observa qué ocupa espacio.
6. Practica `/compact conserva solo la lista de bugs`.

**Comprueba:** existe un `CLAUDE.md` corto y útil, y tras `/clear` Claude sigue conociendo tus reglas.

---

## Lab 3 — Arreglar bugs con verificación (Módulo 3) ⏱ 25 min
Prompt sugerido (cópialo tal cual y compáralo luego con un prompt vago como "arregla los tests"):
```
En curso-claude-code/laboratorio hay tests fallando. Para cada uno:
1) explica la causa raíz en una frase, 2) arregla el CÓDIGO (no los tests),
3) ejecuta `python3 -m unittest -v` hasta que todo pase.
Al final, resume los cambios en una tabla archivo | bug | arreglo.
```
Luego: `haz commit con un mensaje descriptivo`.

**Comprueba:** 7/7 tests pasan; hay un commit; puedes explicar los 4 bugs (compara con `SOLUCIONES.md`).

Extra: pídele `¿qué casos límite no cubren los tests? añade tests para los 3 más importantes`.

---

## Lab 4 — Modo plan y una feature nueva (Módulo 3) ⏱ 30 min
Feature: **exportar el inventario a CSV** y **cupones con fecha de caducidad**.
1. `Shift+Tab` hasta **modo plan**.
2. Prompt:
   ```
   Quiero: (a) Inventario.exportar_csv(ruta) con columnas codigo,nombre,precio,cantidad
   y (b) que los cupones puedan tener fecha de caducidad (si caducó, no aplica).
   Usa TDD: primero tests que fallen, luego la implementación. Propón un plan.
   ```
3. **Corrige el plan** al menos una vez (ej. "usa el módulo csv estándar", "la fecha debe poder inyectarse para testear").
4. Aprueba y deja que implemente.
5. Practica **rewind**: pídele un cambio que no quieras ("renombra todas las funciones al inglés"), y luego `Esc Esc` / `/rewind` para deshacerlo.

**Comprueba:** tests nuevos pasan; entiendes la diferencia entre rewind (checkpoint local) y git.

---

## Lab 5 — Tu propia skill (Módulo 4) ⏱ 20 min
1. Crea `.claude/skills/arregla-tests/SKILL.md` (usa el ejemplo del módulo 4, o pídele a Claude que la cree).
2. Rompe algo a mano (ej. en `descuentos.py` cambia `100` por `1000`).
3. Ejecuta `/arregla-tests`.
4. Crea otra skill `/explica` que reciba un archivo en `$ARGUMENTS` y lo explique para un principiante.
5. Configura permisos: `/permissions` → permite `Bash(python3 -m unittest:*)` para que no te pregunte más.

**Comprueba:** `/arregla-tests` repara el bug sin que escribas nada más, y ya no te pide permiso para los tests.

---

## Lab 6 — Hooks (Módulo 4) ⏱ 20 min
1. Pídele: `configura un hook PostToolUse en .claude/settings.json que ejecute los tests del laboratorio después de cada Edit o Write`.
2. Revisa el JSON generado (compáralo con el del módulo 4). Mira `/hooks`.
3. Pide un cambio pequeño y observa cómo se ejecutan los tests solos.
4. Reto: un hook `PreToolUse` que **bloquee** editar cualquier archivo dentro de `tests/` (salida con código 2 y mensaje en stderr). Pruébalo pidiendo "modifica un test".

**Comprueba:** los tests se ejecutan automáticamente y Claude no puede editar `tests/`. (Quita el bloqueo al terminar si quieres seguir con los labs.)

---

## Lab 7 — Subagentes y MCP (Módulo 5) ⏱ 25 min
1. `Usa un subagente para auditar todo tienda/ buscando más bugs o casos límite (precios negativos, cantidades 0, redondeo) y dame solo la lista priorizada.`
2. Crea con `/agents` (o a mano) el subagente `revisor` del módulo 5.
3. Haz un cambio y pide `usa el subagente revisor sobre mi diff`.
4. Lanza en paralelo: `3 subagentes en paralelo: seguridad, rendimiento y legibilidad de tienda/`.
5. MCP: ejecuta `/mcp` y mira qué hay conectado. Si usas GitHub, prueba a conectar su servidor MCP y pedir `lista mis issues abiertos`.

**Comprueba:** notas que el contexto principal (`/context`) crece poco aunque los subagentes leyeron mucho.

---

## Lab 8 — Proyecto final (Módulo 6) ⏱ 45 min
Aplica todo junto:
1. Rama nueva con worktree: `git worktree add ../tienda-final -b final` y abre `claude` allí.
2. Feature: **historial de pedidos** — `Carrito.confirmar()` descuenta stock del inventario (con `StockInsuficiente` si no hay) y guarda el pedido en una lista con fecha, items y total.
3. Flujo: explorar → modo plan → TDD → implementar → hook de tests activo → subagente revisor → commit.
4. Headless: fuera de la sesión ejecuta
   ```bash
   git diff main | claude -p "revisa este diff y lista riesgos en 5 viñetas"
   ```
5. Pide que suba la rama y abra un PR con descripción.

**Comprueba:** PR abierto, tests en verde, y una revisión headless guardada. 🎓 ¡Terminaste el curso!

---

### Retos extra
- Crea un `CLAUDE.md` global en `~/.claude/` con tus preferencias personales (idioma, estilo).
- Un hook `Stop` que te avise (sonido/notificación) cuando Claude termine.
- Un script que use `claude -p` para generar notas de versión a partir de `git log`.
