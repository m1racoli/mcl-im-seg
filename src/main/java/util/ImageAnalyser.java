package util;

import io.cluster.ImageClustering;
import io.cluster.ImageClusterings;

import java.awt.EventQueue;
import java.awt.Image;
import java.awt.Transparency;

import javax.imageio.ImageIO;
import javax.swing.JFrame;

import java.awt.BorderLayout;

import javax.swing.JFileChooser;
import javax.swing.JMenuBar;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.AbstractAction;
import javax.swing.JScrollPane;

import java.awt.color.ColorSpace;
import java.awt.event.ActionEvent;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;

import javax.swing.Action;


import javax.swing.ScrollPaneConstants;

import model.nb.RadialPixelNeighborhood;

import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;

import java.awt.FlowLayout;

import javax.swing.Box;

public class ImageAnalyser {

	private JFrame frame;
	private final Action fileOpenAction = new OpenFileAction();
	private ImagePanel panel;
	private final Action imageCIELabAction = new CIElabAction();
	private final Action action = new SwingAction_1();
	private final Action action_1 = new CreateABCAction();
	private final Action action_2 = new SwingAction_3();
	private JSpinner scale;
	private JSpinner radius;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
				
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					ImageAnalyser window = new ImageAnalyser();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public ImageAnalyser() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.setBounds(100, 100, 569, 429);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		JMenuBar menuBar = new JMenuBar();
		frame.setJMenuBar(menuBar);
		
		JMenu mnFile = new JMenu("File");
		menuBar.add(mnFile);
		
		JMenuItem mntmOpen = new JMenuItem("Open ...");
		mntmOpen.setAction(fileOpenAction);
		mnFile.add(mntmOpen);
		
		JMenu mnImage = new JMenu("Image");
		menuBar.add(mnImage);
		
		JMenuItem mntmCielab = new JMenuItem("CIELab");
		mntmCielab.setAction(imageCIELabAction);
		mnImage.add(mntmCielab);
		
		JMenuItem mntmConnectivity = new JMenuItem("Connectivity");
		mntmConnectivity.setAction(action);
		mnImage.add(mntmConnectivity);
		
		JMenuItem mntmAbcMatrix = new JMenuItem("abc matrix");
		mntmAbcMatrix.setAction(action_1);
		mnImage.add(mntmAbcMatrix);
		
		JMenuItem mntmOpenClusters = new JMenuItem("open clusters");
		mntmOpenClusters.setAction(action_2);
		mnImage.add(mntmOpenClusters);
		
		
		panel = new ImagePanel();
		JScrollPane scrollPane = new JScrollPane(panel);
		scrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_ALWAYS);
		scrollPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
		frame.getContentPane().add(scrollPane, BorderLayout.CENTER);
		
		JPanel panel_1 = new JPanel();
		scrollPane.setRowHeaderView(panel_1);
		
		Box verticalBox = Box.createVerticalBox();
		panel_1.add(verticalBox);
		
		scale = new JSpinner();
		verticalBox.add(scale);
		scale.setModel(new SpinnerNumberModel(new Double(1.0), new Double(0.1), null, new Double(0.1)));
		
		radius = new JSpinner();
		verticalBox.add(radius);
		radius.setModel(new SpinnerNumberModel(new Double(3.0), new Double(1.0), null, new Double(1.0)));
	}

	private class OpenFileAction extends AbstractAction {
		/**
		 * 
		 */
		private static final long serialVersionUID = -5897181414355425470L;
		public OpenFileAction() {
			putValue(NAME, "Open ...");
			putValue(SHORT_DESCRIPTION, "Open file");
		}
		public void actionPerformed(ActionEvent e) {
			JFileChooser fileChooser = new JFileChooser(".");
			
			if(fileChooser.showOpenDialog(frame) == JFileChooser.APPROVE_OPTION){
				File file = fileChooser.getSelectedFile();
				try {
					BufferedImage image = ImageIO.read(file);
					panel.setImage(image);
				} catch (IOException e1) {
					e1.printStackTrace();
				}				
			}
			
		}
	}
	
	private class CIElabAction extends AbstractAction {
		public CIElabAction() {
			putValue(NAME, "CIELab");
			putValue(SHORT_DESCRIPTION, "Some short description");
		}
		public void actionPerformed(ActionEvent e) {
			
			
			
			BufferedImage image = panel.getImage();			
			
			if(image == null){
				return;
			}
			
			final Raster data = image.getData();			
			//final int w = data.getWidth();
			//final int h = data.getHeight();
			
			//final float[][] data_arrays = new float[3][h*w];
			final CIELab cieLab = CIELab.getInstance();
			//final String[] names = {"L","a","b"};
			//float[] rgb = null;
			//float[] lab = null;
			
//			for(int y = 0; y < h; y++){
//				for(int x = 0; x < w; x++){
//					rgb = data.getPixel(x, y, rgb);
//					lab = cieLab.fromRGB(rgb);
//					final int pos = x + w*y;
//					for(int i = 0; i < 3; i++){
//						data_arrays[i][pos] = (int) lab[i];
//					}
//				}
//			}
			
			final int[] bits = new int[]{8,8,8,8};
			//ColorSpace cs = ColorSpace.getInstance(ColorSpace.CS_GRAY);
			ColorModel cm = new ComponentColorModel(cieLab, bits, false, true, Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
			//SampleModel sm = cm.createCompatibleSampleModel(w, h);
			
			//WritableRaster destRaster = data.createCompatibleWritableRaster();
			
			ColorConvertOp convertOp = new ColorConvertOp(image.getColorModel().getColorSpace(), cieLab, null);
			
			BufferedImage dest = convertOp.createCompatibleDestImage(image, cm);
			convertOp.filter(data, dest.getRaster());
			
			panel.setImage(dest);
			
//			for(int i = 0; i < 3; i++){
//				JFrame frame = new JFrame(names[i]);
//				ImagePanel panel = new ImagePanel();
//				frame.add(panel);
//				DataBufferFloat db = new DataBufferFloat(data_arrays[i], h*w);
//				WritableRaster raster = Raster.createWritableRaster(sm, db, null);
//				BufferedImage im = new BufferedImage(cm, raster, true, null);
//				panel.setImage(im);
//				frame.setVisible(true);
//			}
			
		}
	}
	private class SwingAction_1 extends AbstractAction {
		public SwingAction_1() {
			putValue(NAME, "Connectivity");
			putValue(SHORT_DESCRIPTION, "Some short description");
		}
		public void actionPerformed(ActionEvent e) {
			
			BufferedImage result = ConnectivityGraph.visualize(panel.getImage(), new RadialPixelNeighborhood((Double) radius.getValue()),(Double) scale.getValue());
			
			JFrame frame = new JFrame("Connectivity");
			ImagePanel panel = new ImagePanel();
			panel.setImage(result);
			frame.getContentPane().add(panel);
			frame.setVisible(true);
			
		}
	}
	private class CreateABCAction extends AbstractAction {
		public CreateABCAction() {
			putValue(NAME, "abc Matrix");
			putValue(SHORT_DESCRIPTION, "Some short description");
		}
		public void actionPerformed(ActionEvent e) {
			
			JFileChooser fileChooser = new JFileChooser(".");
			if(fileChooser.showSaveDialog(frame) != JFileChooser.APPROVE_OPTION){
				return;
			}
			
			File output = fileChooser.getSelectedFile();
			try {
				ImageTool.writeABC(output, panel.getImage(), new RadialPixelNeighborhood((Double) radius.getValue()),(Double)scale.getValue());
			} catch (IOException e1) {
				
				e1.printStackTrace();
			}
		}
	}
	private class SwingAction_3 extends AbstractAction {
		public SwingAction_3() {
			putValue(NAME, "open cluster file");
			putValue(SHORT_DESCRIPTION, "Some short description");
		}
		public void actionPerformed(ActionEvent e) {
			
			final BufferedImage image = panel.getImage();
			
			JFileChooser fileChooser = new JFileChooser(".");
			
			if(fileChooser.showOpenDialog(frame) != JFileChooser.APPROVE_OPTION){ 
				return;
			}
			
			File file = fileChooser.getSelectedFile();
			System.out.println("use "+file);
			JFrame frame = new JFrame("Clusters");
			ImagePanel panel = new ImagePanel();
			try {
				ImageClustering clustering = ImageClusterings.read(file, image.getWidth(), image.getHeight());
				BufferedImage result = ImageClusterings.visualize(clustering, image);
				panel.setImage(result);
				//panel.setImage(ImageTool.readClusters(file, image));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			frame.getContentPane().add(panel);
			frame.setVisible(true);
		}
	}
}
