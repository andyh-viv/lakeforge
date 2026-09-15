import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react'
import { api, getToken, setToken, type Me } from './api'

interface AuthState {
  me: Me | null
  loading: boolean
  login: (username: string, password: string) => Promise<void>
  logout: () => void
  refresh: () => Promise<void>
}

const AuthCtx = createContext<AuthState>({
  me: null,
  loading: true,
  login: async () => {},
  logout: () => {},
  refresh: async () => {},
})

export function AuthProvider({ children }: { children: ReactNode }) {
  const [me, setMe] = useState<Me | null>(null)
  const [loading, setLoading] = useState(true)

  const refresh = useCallback(async () => {
    if (!getToken()) {
      setMe(null)
      setLoading(false)
      return
    }
    try {
      setMe(await api.get<Me>('/api/2.0/lakeforge/me'))
    } catch {
      setToken(null)
      setMe(null)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void refresh()
    const onLogout = () => setMe(null)
    window.addEventListener('lakeforge:logout', onLogout)
    return () => window.removeEventListener('lakeforge:logout', onLogout)
  }, [refresh])

  const login = useCallback(
    async (username: string, password: string) => {
      const r = await api.post<{ access_token: string }>('/api/2.0/lakeforge/login', { username, password })
      setToken(r.access_token)
      await refresh()
    },
    [refresh],
  )

  const logout = useCallback(() => {
    setToken(null)
    setMe(null)
  }, [])

  return <AuthCtx.Provider value={{ me, loading, login, logout, refresh }}>{children}</AuthCtx.Provider>
}

export function useAuth() {
  return useContext(AuthCtx)
}
