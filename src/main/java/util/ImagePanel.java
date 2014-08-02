/**
 * 
 */
package util;

import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.LayoutManager;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.color.ColorSpace;
import java.awt.event.MouseEvent;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;

import javax.swing.JPanel;
import javax.swing.ToolTipManager;

/**
 * @author Cedrik
 *
 */
public class ImagePanel extends JPanel {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4046603274107256531L;
	
	private BufferedImage image = null;
	
	/**
	 * 
	 */
	public ImagePanel() {
		ToolTipManager.sharedInstance().registerComponent(this);
	}

	/**
	 * @param layout
	 */
	public ImagePanel(LayoutManager layout) {
		super(layout);
	}

	/**
	 * @param isDoubleBuffered
	 */
	public ImagePanel(boolean isDoubleBuffered) {
		super(isDoubleBuffered);
	}

	/**
	 * @param layout
	 * @param isDoubleBuffered
	 */
	public ImagePanel(LayoutManager layout, boolean isDoubleBuffered) {
		super(layout, isDoubleBuffered);
	}
	
	public BufferedImage getImage() {
		return image;
	}

	public void setImage(BufferedImage image) {
		this.image = image;
		
		if(image == null)
			return;
		
		setPreferredSize(new Dimension(image.getWidth(), image.getHeight()));		
		
		updateUI();
	}

	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		
		if(image == null)
			return;
		
		g.drawImage(image, 0, 0, null);
	}
	
	@Override
	public String getToolTipText(MouseEvent event) {
		
		final Point p = event.getPoint();
		final StringBuilder builder = new StringBuilder(String.format("<html>x: %d, y: %d", p.x,p.y));
		
		if(image == null){
			return builder.append("</html>").toString();
		}
		
		final Raster raster = image.getData();
		final Rectangle rect = raster.getBounds();
		
		if(!rect.contains(p.x, p.y)){
			return builder.append("</html>").toString();
		}
		
		final ColorSpace colorSpace = image.getColorModel().getColorSpace();
		final double[] pixel = raster.getPixel(p.x, p.y, (double[]) null);
		
		for(int i = 0; i < colorSpace.getNumComponents(); i++){
			builder.append(String.format("<br>%s: %f", colorSpace.getName(i), pixel[i]));
		}
		
		return builder.append("</html>").toString();
	}

}
