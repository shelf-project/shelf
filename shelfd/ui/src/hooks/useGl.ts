/** Hand-rolled WebGL2 lifecycle — no three.js, no regl, no glsl-loader.
 *
 * The whole point of overdrive in this UI is to push past the calm
 * operator-console default *without* blowing the bundle. A real
 * dependency like three.js is ~150 KB gz on its own; raw WebGL2 with
 * a single fullscreen quad is ~3 KB of TypeScript. That's the trade
 * — a little plumbing here in exchange for keeping the SPA distroless-
 * image-friendly (`shelf/shelfd/ui/vite.config.ts` budget = ~100 KB gz).
 *
 * Contract:
 *   - Caller owns a `<canvas>` ref and a fragment shader source string.
 *   - `useGl` compiles the program, sets up a single VAO + VBO holding
 *     a fullscreen NDC quad, exposes a `draw(uniforms)` callback that
 *     runs at most once per RAF, handles `webglcontextlost/restored`,
 *     resizes with the canvas, and tears everything down on unmount.
 *   - Reduced-motion / off-screen pauses are *callers'* responsibility
 *     (they hold the `useReducedMotion` and `useIntersection` checks).
 *     This hook only cares about WebGL itself.
 *
 * The vertex shader is fixed: pass NDC through, expose `vUv` in [0,1].
 * Caller's fragment shader can use `uniform vec2 uRes`, `uniform float uTime`,
 * plus whatever extra uniforms it declares — `setUniforms` introspects
 * the program once at compile time so unknown names become no-ops
 * silently rather than throwing.
 */

import { RefObject, useEffect, useRef } from "react";

const VS = /* glsl */ `#version 300 es
in vec2 aPos;
out vec2 vUv;
void main() {
  vUv = aPos * 0.5 + 0.5;
  gl_Position = vec4(aPos, 0.0, 1.0);
}`;

type UniformValue = number | [number, number] | [number, number, number] | [number, number, number, number] | Float32Array;

export type GlHandle = {
  draw: (uniforms: Record<string, UniformValue>) => void;
  resize: () => void;
};

export function useGl(
  canvasRef: RefObject<HTMLCanvasElement>,
  fragmentSource: string,
  enabled: boolean,
): RefObject<GlHandle | null> {
  const handleRef = useRef<GlHandle | null>(null);

  useEffect(() => {
    if (!enabled) {
      handleRef.current = null;
      return;
    }
    const canvas = canvasRef.current;
    if (!canvas) return;
    const gl = canvas.getContext("webgl2", { antialias: false, premultipliedAlpha: false, alpha: true });
    if (!gl) {
      // Browser doesn't support WebGL2 (or it's blocked). Caller must
      // already have rendered the static fallback; we just stay quiet.
      handleRef.current = null;
      return;
    }

    const program = compile(gl, VS, fragmentSource);
    if (!program) {
      handleRef.current = null;
      return;
    }

    const vao = gl.createVertexArray();
    const vbo = gl.createBuffer();
    if (!vao || !vbo) return;
    gl.bindVertexArray(vao);
    gl.bindBuffer(gl.ARRAY_BUFFER, vbo);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]), gl.STATIC_DRAW);
    const aPosLoc = gl.getAttribLocation(program, "aPos");
    if (aPosLoc < 0) return;
    gl.enableVertexAttribArray(aPosLoc);
    gl.vertexAttribPointer(aPosLoc, 2, gl.FLOAT, false, 0, 0);

    // Introspect every active uniform once. Unknown uniform names in
    // `draw({ ... })` are skipped silently — keeps callers cheap.
    const uniforms: Record<string, { loc: WebGLUniformLocation | null; type: number }> = {};
    const count = gl.getProgramParameter(program, gl.ACTIVE_UNIFORMS) as number;
    for (let i = 0; i < count; i++) {
      const info = gl.getActiveUniform(program, i);
      if (!info) continue;
      const loc = gl.getUniformLocation(program, info.name);
      uniforms[info.name] = { loc, type: info.type };
    }

    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      const w = Math.floor(canvas.clientWidth * dpr);
      const h = Math.floor(canvas.clientHeight * dpr);
      if (canvas.width !== w || canvas.height !== h) {
        canvas.width = w;
        canvas.height = h;
      }
      gl.viewport(0, 0, canvas.width, canvas.height);
    };
    resize();

    const onLost = (e: Event) => e.preventDefault();
    canvas.addEventListener("webglcontextlost", onLost, false);

    const draw = (vals: Record<string, UniformValue>) => {
      resize();
      gl.useProgram(program);
      gl.bindVertexArray(vao);
      // Always feed uRes / uTime if the shader declares them.
      const auto: Record<string, UniformValue> = {
        uRes: [canvas.width, canvas.height],
        uTime: performance.now() / 1000,
        ...vals,
      };
      for (const [name, value] of Object.entries(auto)) {
        const u = uniforms[name];
        if (!u || !u.loc) continue;
        setUniform(gl, u.loc, u.type, value);
      }
      gl.clearColor(0, 0, 0, 0);
      gl.clear(gl.COLOR_BUFFER_BIT);
      gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    };

    handleRef.current = { draw, resize };

    return () => {
      handleRef.current = null;
      canvas.removeEventListener("webglcontextlost", onLost);
      gl.deleteBuffer(vbo);
      gl.deleteVertexArray(vao);
      gl.deleteProgram(program);
    };
  }, [canvasRef, fragmentSource, enabled]);

  return handleRef;
}

function compile(gl: WebGL2RenderingContext, vs: string, fs: string): WebGLProgram | null {
  const v = gl.createShader(gl.VERTEX_SHADER);
  const f = gl.createShader(gl.FRAGMENT_SHADER);
  if (!v || !f) return null;
  gl.shaderSource(v, vs);
  gl.shaderSource(f, fs);
  gl.compileShader(v);
  gl.compileShader(f);
  if (!gl.getShaderParameter(v, gl.COMPILE_STATUS)) {
    // eslint-disable-next-line no-console
    console.warn("[useGl] vertex shader compile failed:", gl.getShaderInfoLog(v));
    return null;
  }
  if (!gl.getShaderParameter(f, gl.COMPILE_STATUS)) {
    // eslint-disable-next-line no-console
    console.warn("[useGl] fragment shader compile failed:", gl.getShaderInfoLog(f));
    return null;
  }
  const p = gl.createProgram();
  if (!p) return null;
  gl.attachShader(p, v);
  gl.attachShader(p, f);
  gl.linkProgram(p);
  if (!gl.getProgramParameter(p, gl.LINK_STATUS)) {
    // eslint-disable-next-line no-console
    console.warn("[useGl] program link failed:", gl.getProgramInfoLog(p));
    return null;
  }
  gl.deleteShader(v);
  gl.deleteShader(f);
  return p;
}

function setUniform(
  gl: WebGL2RenderingContext,
  loc: WebGLUniformLocation,
  type: number,
  value: UniformValue,
) {
  switch (type) {
    case gl.FLOAT:
      gl.uniform1f(loc, value as number);
      return;
    case gl.FLOAT_VEC2: {
      const v = value as [number, number];
      gl.uniform2f(loc, v[0], v[1]);
      return;
    }
    case gl.FLOAT_VEC3: {
      const v = value as [number, number, number];
      gl.uniform3f(loc, v[0], v[1], v[2]);
      return;
    }
    case gl.FLOAT_VEC4: {
      const v = value as [number, number, number, number];
      gl.uniform4f(loc, v[0], v[1], v[2], v[3]);
      return;
    }
    case gl.INT:
    case gl.SAMPLER_2D:
      gl.uniform1i(loc, value as number);
      return;
    default: {
      // Vector-of-floats (Float32Array) fallback for ripples / colormaps.
      if (value instanceof Float32Array) {
        gl.uniform1fv(loc, value);
      }
    }
  }
}
