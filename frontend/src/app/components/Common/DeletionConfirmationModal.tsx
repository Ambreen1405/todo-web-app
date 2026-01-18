import React, { useEffect } from 'react';

interface DeletionConfirmationModalProps {
  isOpen: boolean;
  taskTitle: string;
  onClose: () => void;
}

const DeletionConfirmationModal: React.FC<DeletionConfirmationModalProps> = ({
  isOpen,
  taskTitle,
  onClose
}) => {
  useEffect(() => {
    let timer: NodeJS.Timeout;

    if (isOpen) {
      // Auto-close the modal after 3 seconds
      timer = setTimeout(() => {
        onClose();
      }, 3000); // 3 seconds
    }

    // Cleanup function to clear the timer if component unmounts early
    return () => {
      if (timer) {
        clearTimeout(timer);
      }
    };
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black bg-opacity-50"
      ></div>

      {/* Modal Content */}
      <div className="relative bg-white rounded-lg shadow-xl p-6 max-w-md w-full mx-4">
        <div className="text-center">
          <div className="text-2xl font-bold text-red-600 mb-2">Deleted</div>
          <h3 className="text-lg font-medium text-gray-900 mb-4">
            {taskTitle}
          </h3>
          <p className="text-sm text-gray-500">This message will close automatically</p>
        </div>
      </div>
    </div>
  );
};

export default DeletionConfirmationModal;