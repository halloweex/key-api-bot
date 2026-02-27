import { useState } from 'react'
import Avatar from 'boring-avatars'

const AVATAR_COLORS = ['#8B5CF6', '#2563EB', '#7C3AED', '#3B82F6', '#6366F1']

interface UserAvatarProps {
  name: string
  photoUrl?: string | null
  size?: number
  className?: string
}

export function UserAvatar({ name, photoUrl, size = 32, className = '' }: UserAvatarProps) {
  const [imageError, setImageError] = useState(false)

  return (
    <div
      className={`relative flex-shrink-0 ${className}`}
      style={{ width: size, height: size }}
    >
      {/* Generated avatar — always rendered as base layer */}
      <div className="absolute inset-0 rounded-full overflow-hidden">
        <Avatar
          size={size}
          name={name}
          variant="beam"
          colors={AVATAR_COLORS}
        />
      </div>

      {/* Photo overlay — only if available and not errored */}
      {photoUrl && !imageError && (
        <img
          src={photoUrl}
          alt=""
          className="absolute inset-0 rounded-full object-cover border-2 border-white shadow-sm"
          style={{ width: size, height: size }}
          onError={() => setImageError(true)}
        />
      )}
    </div>
  )
}
