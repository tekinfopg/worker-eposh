#!/usr/bin/env python3
"""
Download and prepare InsightFace models for face analysis.
This script ensures models are properly extracted and initialized.
"""
import os
from insightface.app import FaceAnalysis

print("Downloading and preparing InsightFace antelopev2 model...")

# Set the model root directory
model_root = '.insightface'
os.makedirs(model_root, exist_ok=True)

# Download and prepare the models
# The prepare() method will download, extract, and verify the models
try:
    app = FaceAnalysis(name='antelopev2', root=model_root, allowed_modules=['detection', 'recognition'])
    app.prepare(ctx_id=0, det_size=(640, 640))
    print("✓ InsightFace models downloaded and prepared successfully!")
except Exception as e:
    print(f"✗ Error preparing InsightFace models: {e}")
    # Try alternative approach - just trigger the download without full initialization
    print("Attempting alternative download method...")
    from insightface.model_zoo import model_zoo
    model_zoo.get_model('antelopev2', root=model_root)
    print("✓ Models downloaded (initialization will happen at runtime)")
