# Módulo 3 — Flujo de trabajo profesional

## 3.1 El ciclo que funciona: Explorar → Planificar → Codificar → Verificar
1. **Explorar**: "Lee `tienda/` y explícame cómo se calcula el total. No escribas código todavía."
2. **Planificar**: entra en **modo plan** (`Shift+Tab` hasta ver *plan mode*). Claude investiga y propone un plan sin tocar nada. Tú lo corriges.
3. **Codificar**: aprueba el plan; Claude implementa.
4. **Verificar**: tests, linter, ejecutar la app. Si no hay forma de verificar, **pide primero que la cree**.
5. **Commit**: "haz commit con un mensaje descriptivo".

Para cambios pequeños y obvios puedes saltarte el plan. Para cualquier cosa que toque varios archivos o tenga decisiones de diseño, planifica.

## 3.2 TDD con Claude (muy efectivo)
1. "Escribe tests para X. Aún no existe la implementación; no la crees."
2. "Ejecútalos y confirma que fallan."
3. "Haz commit de los tests."
4. "Ahora implementa hasta que pasen, sin modificar los tests."

Los tests son el "objetivo" que Claude puede comprobar solo, iterando.

## 3.3 Pensar más
Para problemas difíciles pide explícitamente que razone más ("piensa con cuidado las alternativas antes de decidir") y usa el modo plan. En `/model` y `/config` puedes ajustar modelo y nivel de razonamiento.

## 3.4 Corregir el rumbo
- `Esc` en cuanto veas que va por mal camino, y explica qué cambiar.
- `Esc Esc` / `/rewind` para volver a un punto anterior (conversación y/o cambios de código — son *checkpoints* locales, **no** sustituyen a git).
- "Deshaz lo último y prueba otro enfoque: ..."

## 3.5 Pedir revisiones
- `/review` o la skill `/code-review` (según versión) para revisar tu diff.
- `/security-review` para una revisión de seguridad de los cambios.
- Truco: después de implementar, `/clear` y pide "revisa el diff de la rama actual como un revisor exigente". Una conversación limpia revisa mejor que la que escribió el código.

➡️ **Labs 3 y 4**.
