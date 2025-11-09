import sys
import time
import json
import os
from PyQt5.QtWidgets import QApplication, QWidget, QTextEdit, QVBoxLayout, QPushButton, QLabel, QHBoxLayout, QLineEdit
from PyQt5.QtCore import Qt, QPoint
from PyQt5.QtGui import QPalette, QColor
import pyautogui
from screeninfo import get_monitors

# Set a global shorter pause for all pyautogui calls.
pyautogui.PAUSE = 0.05

# Which monitor index to target? (0-based indexing)
MONITOR_INDEX = 2

# Config file path
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tvtc2000_config.json')

def move_click_type_at_monitor2(ticker):
    monitors = get_monitors()
    if MONITOR_INDEX >= len(monitors):
        print(f"Error: Monitor index {MONITOR_INDEX} not available (only found {len(monitors)})")
        return

    m = monitors[MONITOR_INDEX]
    center_x = m.x + m.width // 2
    center_y = m.y + m.height // 2
    print(f"Target monitor #{MONITOR_INDEX}: (x={m.x}, y={m.y}), size={m.width}x{m.height}")
    print(f"Calculated center: ({center_x}, {center_y})")

    # Move to the center of the target monitor
    pyautogui.moveTo(center_x, center_y, duration=0.1)
    
    # Click first
    pyautogui.click()
    
    # Pause for the same duration as pyautogui.PAUSE
    time.sleep(pyautogui.PAUSE)
    
    # Now type the ticker
    pyautogui.write(ticker.strip().upper(), interval=0.02)
    pyautogui.press('enter')
    print(f"Sent ticker '{ticker.strip().upper()}' at monitor {MONITOR_INDEX} center.")

class DragDropTextEdit(QTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        palette = self.palette()
        palette.setColor(QPalette.Base, QColor("#2D2D2D"))
        palette.setColor(QPalette.Text, QColor("#E0E0E0"))
        self.setPalette(palette)
        self.setStyleSheet("font: 14pt Arial;")

    def dragEnterEvent(self, event):
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dropEvent(self, event):
        dropped_text = event.mimeData().text().strip()
        if dropped_text:
            ticker = dropped_text.splitlines()[0].strip()
            if ticker:
                move_click_type_at_monitor2(ticker)
                print(f"Dropped ticker: '{ticker}'")
            else:
                print("No valid ticker found.")
            self.clear()
        event.acceptProposedAction()

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("TickerMover (Monitor #2)")
        self.resize(400, 300)
        palette = self.palette()
        palette.setColor(QPalette.Window, QColor("#2D2D2D"))
        self.setPalette(palette)
        layout = QVBoxLayout()

        label = QLabel("Drop Ticker(s) from TC2000 Here")
        label.setStyleSheet("font: 16pt Arial; color: #E0E0E0;")
        label.setAlignment(Qt.AlignCenter)
        layout.addWidget(label)

        # Add manual input field
        input_layout = QHBoxLayout()
        self.ticker_input = QLineEdit()
        self.ticker_input.setStyleSheet("background-color: #3D3D3D; color: #E0E0E0; font: 14pt Arial; padding: 5px;")
        self.ticker_input.setPlaceholderText("Enter ticker symbol")
        input_layout.addWidget(self.ticker_input)
        
        self.send_button = QPushButton("Send")
        self.send_button.setStyleSheet("background-color: #3D3D3D; color: #E0E0E0;")
        self.send_button.clicked.connect(self.send_ticker)
        input_layout.addWidget(self.send_button)
        
        layout.addLayout(input_layout)

        self.text_edit = DragDropTextEdit()
        layout.addWidget(self.text_edit)

        self.copy_button = QPushButton("Copy Tickers")
        self.copy_button.setStyleSheet("background-color: #3D3D3D; color: #E0E0E0;")
        self.copy_button.clicked.connect(self.copy_tickers)
        layout.addWidget(self.copy_button)

        # Create a horizontal layout for the bottom buttons
        button_layout = QHBoxLayout()
        
        self.clear_button = QPushButton("Clear")
        self.clear_button.setStyleSheet("background-color: #3D3D3D; color: #E0E0E0;")
        self.clear_button.clicked.connect(self.clear_text)
        button_layout.addWidget(self.clear_button)
        
        self.save_pos_button = QPushButton("Save Position")
        self.save_pos_button.setStyleSheet("background-color: #3D3D3D; color: #E0E0E0;")
        self.save_pos_button.clicked.connect(self.save_position)
        button_layout.addWidget(self.save_pos_button)
        
        layout.addLayout(button_layout)

        self.setLayout(layout)
        
        # Connect enter key press to send ticker
        self.ticker_input.returnPressed.connect(self.send_ticker)
        
        # Load saved position if available
        self.load_position()

    def send_ticker(self):
        """Send the manually entered ticker to the target monitor"""
        ticker = self.ticker_input.text().strip()
        if ticker:
            move_click_type_at_monitor2(ticker)
            self.ticker_input.clear()
            self.ticker_input.setFocus()
        else:
            print("No ticker entered.")

    def copy_tickers(self):
        text = self.text_edit.toPlainText().strip()
        if text:
            QApplication.clipboard().setText(text)
            print("Tickers copied to clipboard.")
        else:
            print("No tickers to copy.")

    def clear_text(self):
        self.text_edit.clear()
        print("Ticker log cleared.")
        
    def save_position(self):
        """Save the current window position to config file"""
        config = {}
        pos = self.pos()
        config['position'] = {'x': pos.x(), 'y': pos.y()}
        
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(config, f)
            print(f"Window position saved: {pos.x()}, {pos.y()}")
        except Exception as e:
            print(f"Error saving position: {e}")
    
    def load_position(self):
        """Load the saved window position from config file"""
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                
                if 'position' in config:
                    x = config['position']['x']
                    y = config['position']['y']
                    self.move(x, y)
                    print(f"Window position loaded: {x}, {y}")
        except Exception as e:
            print(f"Error loading position: {e}")
            
    def closeEvent(self, event):
        """Automatically save position when closing the window"""
        self.save_position()
        super().closeEvent(event)

if __name__ == "__main__":
    from screeninfo import get_monitors  # ensure import is here for monitor detection
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
