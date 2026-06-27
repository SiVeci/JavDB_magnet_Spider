/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './index.html',
    './src/**/*.{vue,js,ts,jsx,tsx}',
  ],
  theme: {
    extend: {
      colors: {
        primary: {
          DEFAULT: 'var(--c-primary)',
          hover: 'var(--c-primary-hover)',
          soft: 'var(--c-primary-soft)',
          text: 'var(--c-primary-text)',
          ring: 'var(--c-primary-ring)',
        },
        success: {
          DEFAULT: 'var(--c-success)',
          soft: 'var(--c-success-soft)',
          text: 'var(--c-success-text)',
        },
        warning: {
          DEFAULT: 'var(--c-warning)',
          soft: 'var(--c-warning-soft)',
          text: 'var(--c-warning-text)',
        },
        danger: {
          DEFAULT: 'var(--c-danger)',
          soft: 'var(--c-danger-soft)',
          text: 'var(--c-danger-text)',
        },
        info: {
          DEFAULT: 'var(--c-info)',
          soft: 'var(--c-info-soft)',
          text: 'var(--c-info-text)',
        },
        neutral: {
          DEFAULT: 'var(--c-neutral)',
          soft: 'var(--c-neutral-soft)',
          text: 'var(--c-neutral-text)',
        },
        surface: {
          DEFAULT: 'var(--c-surface)',
          sunken: 'var(--c-surface-sunken)',
        },
      },
      fontFamily: {
        display: ['var(--font-display)'],
        sans: ['var(--font-sans)'],
        mono: ['var(--font-mono)'],
      },
      borderRadius: {
        DEFAULT: 'var(--radius)',
        sm: 'var(--radius-sm)',
        lg: 'var(--radius-lg)',
        xl: 'var(--radius-xl)',
        full: 'var(--radius-full)',
      },
      boxShadow: {
        DEFAULT: 'var(--shadow)',
        sm: 'var(--shadow-sm)',
        md: 'var(--shadow-md)',
        pop: 'var(--shadow-pop)',
      },
    },
  },
  plugins: [],
}

