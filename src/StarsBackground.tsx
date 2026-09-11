import React, { useEffect, useRef } from 'react';

interface Star {
  x: number;
  y: number;
  size: number;
  speedX: number;
  speedY: number;
  baseX: number;
  baseY: number;
}

const StarsBackground: React.FC = () => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const mouseRef = useRef({ x: -1000, y: -1000 });

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    let animationFrameId: number;
    let stars: Star[] = [];

    const initCanvas = () => {
      canvas.width = window.innerWidth;
      canvas.height = window.innerHeight;
      createStars();
    };

    const createStars = () => {
      stars = [];
      const numStars = Math.floor((canvas.width * canvas.height) / 3000); // adjust density
      for (let i = 0; i < numStars; i++) {
        const x = Math.random() * canvas.width;
        const y = Math.random() * canvas.height;
        stars.push({
          x,
          y,
          baseX: x,
          baseY: y,
          size: Math.random() * 2 + 0.5,
          speedX: (Math.random() - 0.5) * 0.5,
          speedY: (Math.random() - 0.5) * 0.5,
        });
      }
    };

    const drawStars = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      
      const mouseX = mouseRef.current.x;
      const mouseY = mouseRef.current.y;
      
      stars.forEach((star) => {
        // Subtle natural drift
        star.baseX += star.speedX;
        star.baseY += star.speedY;

        // Wrap around edges
        if (star.baseX > canvas.width) star.baseX = 0;
        else if (star.baseX < 0) star.baseX = canvas.width;
        
        if (star.baseY > canvas.height) star.baseY = 0;
        else if (star.baseY < 0) star.baseY = canvas.height;

        // Parallax effect towards mouse
        const dx = mouseX - star.baseX;
        const dy = mouseY - star.baseY;
        const distance = Math.sqrt(dx * dx + dy * dy);
        
        let offsetX = 0;
        let offsetY = 0;

        if (distance < 300 && mouseX !== -1000) {
          const force = (300 - distance) / 300;
          // Invert the force so stars move away or towards. Moving towards feels like "moving stars when cursor goes"
          // Let's make them move slightly away for a repulsion effect, or slightly along the mouse
          offsetX = -dx * force * 0.1;
          offsetY = -dy * force * 0.1;
        }

        star.x = star.baseX + offsetX;
        star.y = star.baseY + offsetY;

        ctx.fillStyle = `rgba(16, 185, 129, ${Math.random() * 0.5 + 0.3})`; // Green stars
        ctx.beginPath();
        ctx.arc(star.x, star.y, star.size, 0, Math.PI * 2);
        ctx.fill();
      });

      animationFrameId = requestAnimationFrame(drawStars);
    };

    const handleMouseMove = (e: MouseEvent) => {
      mouseRef.current = { x: e.clientX, y: e.clientY };
    };
    
    const handleMouseLeave = () => {
      mouseRef.current = { x: -1000, y: -1000 };
    };

    window.addEventListener('resize', initCanvas);
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseleave', handleMouseLeave);

    initCanvas();
    drawStars();

    return () => {
      window.removeEventListener('resize', initCanvas);
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseleave', handleMouseLeave);
      cancelAnimationFrame(animationFrameId);
    };
  }, []);

  return (
    <canvas 
      ref={canvasRef} 
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        width: '100vw',
        height: '100vh',
        zIndex: -1,
        pointerEvents: 'none'
      }} 
    />
  );
};

export default StarsBackground;
